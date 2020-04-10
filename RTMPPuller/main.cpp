/*
 * Copyright (C) 2020 BrianYi, All rights reserved
 */

#include <iostream>
#include <vector>
#include <thread>
#include <string>
#include <queue>
#include <mutex>
#include "common.h"
#include "TcpSocket.h"
#include "Log.h"
 //#define TIME_CACULATE
#include "Packet.h"


//#pragma comment(linker, "/SUBSYSTEM:windows /ENTRY:mainCRTStartup")

#define SERVER_IP "192.168.1.105"
#define SERVER_PORT 5566

enum
{
	STREAMING_START,
	STREAMING_IN_PROGRESS,
	STREAMING_STOPPING,
	STREAMING_STOPPED
};

enum
{
	TypeData,
	TypeFin
};

struct Frame
{
	int type;
	int64_t timestamp;
	int32_t size;
	char *data;
};

typedef std::queue<Frame> FrameData;
struct StreamInfo
{
	std::string app;
	int timebase;
	FrameData frameData;
};

struct STREAMING_PULLER
{
	TcpSocket conn;
	int state;
	StreamInfo stream;
	std::string filePath;
	std::mutex mux;
	StatisticInfo stat;
	epoll_event ev_r;
	epoll_event ev_w;
	int efd;
};

bool init_sockets( )
{
#ifdef WIN32
	WORD version = MAKEWORD( 1, 1 );
	WSADATA wsaData;
	return ( WSAStartup( version, &wsaData ) == 0 );
#endif
	return true;
}

void cleanup_sockets( )
{
#ifdef WIN32
	WSACleanup( );
#endif
}

void
stopStreaming( STREAMING_PULLER * puller )
{
	if ( puller->state != STREAMING_STOPPED )
	{
		if ( puller->state == STREAMING_IN_PROGRESS )
		{
			puller->state = STREAMING_STOPPING;

			// wait for streaming threads to exit
			while ( puller->state != STREAMING_STOPPED )
				msleep( 10 );
		}
		puller->state = STREAMING_STOPPED;
	}
}

#define MAX_EVENTS 10
int thread_func_for_receiver( void *arg )
{
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is start..." );
	STREAMING_PULLER* puller = ( STREAMING_PULLER* ) arg;
	StreamInfo& stream = puller->stream;
	bool isAck = false;
	struct epoll_event events[ MAX_EVENTS ];
	while ( !isAck )
	{
		epoll_ctl( puller->efd, EPOLL_CTL_MOD, puller->conn.fSocket, &puller->ev_w );
		int nfds = epoll_wait( puller->efd, events, MAX_EVENTS, -1 );
		if (send_play_packet( puller->conn,
						  get_current_milli( ),
						  puller->stream.app.c_str( ), Socket::NonBlocking ) <= 0)
		{
			continue;
		}
		
		// recv ack
		epoll_ctl( puller->efd, EPOLL_CTL_MOD, puller->conn.fSocket, &puller->ev_r );
		epoll_wait( puller->efd, events, MAX_EVENTS, -1 );
		PACKET pkt;
		if ( recv_packet( puller->conn, pkt, Socket::NonBlocking ) <= 0 )
			continue;
		switch ( pkt.header.type )
		{
		case Ack:
			printf( "Begin to receive stream.\n" );
			puller->stream.timebase = pkt.header.reserved;
			isAck = true;
			break;
		case Err:
			//printf( "Server has no stream for name %s\n", pkt.header.app );
			msleep( 10 );
			break;
		default:
			printf( "unknown packet.\n" );
		}
	}

	auto cmp = [ ] ( PACKET& a, PACKET& b ) { return a.header.seq > b.header.seq; };
	std::priority_queue<PACKET, std::vector<PACKET>, decltype( cmp )> priq( cmp );
	Frame frame;
	bzero( &frame, sizeof (Frame) );
	int32_t totalSize = 0;
	bool isReceiveFinished = false;
	epoll_ctl( puller->efd, EPOLL_CTL_MOD, puller->conn.fSocket, &puller->ev_r );
	while ( puller->state == STREAMING_START &&
			!isReceiveFinished )
	{
		PACKET pkt;
		epoll_wait( puller->efd, events, MAX_EVENTS, -1 );
		if ( recv_packet( puller->conn, pkt, Socket::NonBlocking ) <= 0 ) // no packet, continue loop next
			continue;

		if ( INVALID_PACK( pkt.header ) )
		{
			RTMP_LogAndPrintf( RTMP_LOGERROR, "Invalid packet %s:%d", __FUNCTION__, __LINE__ );
			continue;
		}
		caculate_statistc( puller->stat, pkt, StatRecv );

		switch ( pkt.header.type )
		{
		case Pull:
		{
			if ( stream.app != pkt.header.app )
			{
				send_err_packet( puller->conn,
								 get_current_milli( ),
								 pkt.header.app );
			}

			// throw incomplete packet
			if ( priq.empty( ) && pkt.header.seq != 0 )
			{
				RTMP_Log( RTMP_LOGDEBUG, "incomplete packet, throw out.", priq.size( ), totalSize, frame.size );
				continue;
			}

			if ( priq.empty( ) )
			{
				frame.type = TypeData;
				frame.timestamp = pkt.header.timestamp;
				frame.size = pkt.header.size;
				frame.data = ( char * ) malloc( frame.size );
			}

			if ( frame.timestamp == pkt.header.timestamp )
			{
				priq.push( pkt );
				totalSize += BODY_SIZE_H( pkt.header );
			}
			else
			{
				RTMP_LogAndPrintf( RTMP_LOGERROR, "recv packet is incomplete %s:%d", __FUNCTION__, __LINE__ );
				continue;
			}


			// full frame
			if ( totalSize == frame.size )
			{
				PACKET tmpPack;
				int idx = 0;
				totalSize = 0;
				while ( !priq.empty( ) )
				{
					tmpPack = priq.top( );
					priq.pop( );
					int bodySize = BODY_SIZE_H( tmpPack.header );
					memcpy( &frame.data[ idx ], tmpPack.body, bodySize );
					idx += bodySize;
				}
				std::unique_lock<std::mutex> lock( puller->mux );
				stream.frameData.push( frame );
			}
			break;
		}
		case Fin:
		{
			if ( stream.app != pkt.header.app )
			{
				send_err_packet( puller->conn,
								 get_current_milli( ),
								 pkt.header.app );
				break;
			}
			frame.type = TypeFin;
			frame.timestamp = pkt.header.timestamp;
			frame.size = pkt.header.size;
			//frame.data
			std::unique_lock<std::mutex> lock( puller->mux );
			stream.frameData.push( frame );
			lock.unlock( );
			isReceiveFinished = true;
			break;
		}
		default:
			RTMP_Log( RTMP_LOGDEBUG, "unknown packet." );
			break;
		}
	}
	RTMP_Log( RTMP_LOGDEBUG, "receiver thread is quit." );
	return true;
}

int thread_func_for_writer( void *arg )
{
	RTMP_LogPrintf( "writer thread is start...\n" );
	STREAMING_PULLER *puller = ( STREAMING_PULLER * ) arg;

	FILE *fp = fopen( puller->filePath.c_str( ), "wb" );
	if ( !fp )
	{
		RTMP_LogPrintf( "Open File Error.\n" );
		stopStreaming( puller );
		return -1;
	}

	FrameData& frameData = puller->stream.frameData;
	int64_t currentTime = 0, waitTime = 0;
	while ( puller->state == STREAMING_START )
	{
#ifdef _DEBUG
		TIME_BEG( 1 );
#endif // _DEBUG
		if ( frameData.empty( ) )
		{
			msleep( 10 );
			continue;
		}
#ifdef _DEBUG
		TIME_END( 1 );
#endif // _DEBUG

#ifdef _DEBUG
		TIME_BEG( 2 );	//134ms 134ms 134ms 134ms
#endif // _DEBUG
		std::unique_lock<std::mutex> lock( puller->mux );
		Frame frame = frameData.front( );
		frameData.pop( );
		lock.unlock( );

		if ( frame.type == TypeFin )
		{
			stopStreaming( puller );
			break;
		}

//		currentTime = get_current_milli( );
//		waitTime = frame.timestamp - currentTime;
// 		if ( waitTime >= 0 )
// 			msleep( waitTime );
		fwrite( frame.data, 1, frame.size, fp );
#ifdef _DEBUG
		TIME_END( 2 );
#endif // _DEBUG

		int64_t writeTimestamp = get_current_milli( );
		RTMP_Log( RTMP_LOGDEBUG, "write frame %dB, frame timestamp=%lld, write timestamp=%lld, W-F=%lld, frameData.size=%d",
				  frame.size,
				  frame.timestamp,
				  writeTimestamp,
				  writeTimestamp - frame.timestamp,
				  frameData.size());

		free( frame.data );
	}

	fclose( fp );
	RTMP_LogPrintf( "writer thread is quit.\n" );
	return true;
}

void show_statistics( STREAMING_PULLER* puller )
{
	printf( "%-15s%-6s%-8s%-20s %-8s\t\t%-13s\t%-10s\t%-15s\t %-8s\t%-13s\t%-10s\t%-15s\n",
			"ip", "port", "type", "app",
			"rec-byte", "rec-byte-rate", "rec-packet", "rec-packet-rate",
			"snd-byte", "snd-byte-rate", "snd-packet", "snd-packet-rate" );


	printf( "%-15s%-6d%-8s%-20s %-6.2fMB\t\t%-9.2fKB/s\t%-10lld\t%-13lld/s\t %-6.2fMB\t%-9.2fKB/s\t%-10lld\t%-13lld/s\n",
			puller->conn.ip( ).c_str( ),
			puller->conn.port( ),
			"Puller",
			puller->stream.app.c_str( ),

			MB( puller->stat.recvBytes ),
			KB( puller->stat.recvByteRate ),
			puller->stat.recvPackets,
			puller->stat.recvPacketRate,

			MB( puller->stat.sendBytes ),
			KB( puller->stat.sendByteRate ),
			puller->stat.sendPackets,
			puller->stat.sendPacketRate );

}

int thread_func_for_controller( void *arg )
{
	RTMP_LogPrintf( "controller thread is start...\n" );
	STREAMING_PULLER *puller = ( STREAMING_PULLER * ) arg;
	std::string choice;
	while ( puller->state == STREAMING_START )
	{
		//system( "cls" );
		//show_statistics( puller );
		msleep( 1000 );
	}
	RTMP_LogPrintf( "controller thread is quit.\n" );
	return 0;
}

int main( int argc, char* argv[ ] )
{
	if ( argc < 3 )
	{
		printf( "please pass in live name and file path parameter.\n" );
		printf( "usage: puller 'live-name' '/path/to/save/file' \n" );
		return 0;
	}
	FILE* dumpfile = nullptr;
	if ( argv[ 3 ] )
		dumpfile = fopen( argv[ 3 ], "a+" );
	else
		dumpfile = fopen( "rtmp_puller.dump", "a+" );
	RTMP_LogSetOutput( dumpfile );
	RTMP_LogSetLevel( RTMP_LOGALL );
	RTMP_LogThreadStart( );

#if WIN32
	SYSTEMTIME tm;
	GetSystemTime( &tm );
#elif __linux__
	time_t t = time( NULL );
	struct tm tm = *localtime( &t );
#define wYear tm_year+1900
#define wMonth tm_mon+1
#define wDay tm_mday
#define wHour tm_hour
#define wMinute tm_min
#define wSecond tm_sec
#define wMilliseconds 0
#endif
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
	RTMP_Log( RTMP_LOGDEBUG, "log file:\trtmp_puller.dump" );
	RTMP_Log( RTMP_LOGDEBUG, "log timestamp:\t%lld", get_timestamp_ms( ) );
	RTMP_Log( RTMP_LOGDEBUG, "log date:\t%d-%d-%d %d:%d:%d",
			  tm.wYear,
			  tm.wMonth,
			  tm.wDay,
			  tm.wHour, tm.wMinute, tm.wSecond );
	RTMP_Log( RTMP_LOGDEBUG, "==============================" );
	init_sockets( );

	STREAMING_PULLER *puller = new STREAMING_PULLER;
	puller->state = STREAMING_START;
	puller->stream.app = argv[ 1 ];
	puller->filePath = argv[ 2 ];
	puller->conn.set_socket_rcvbuf_size( 10 * MAX_PACKET_SIZE );
	puller->conn.set_socket_sndbuf_size( 10 * MAX_PACKET_SIZE );
	puller->efd = epoll_create( MAX_EVENTS );
	puller->ev_r.data.fd = puller->conn.fSocket;
	puller->ev_r.events = EPOLLIN;
	puller->ev_w.data.fd = puller->conn.fSocket;
	puller->ev_w.events = EPOLLOUT;
	bzero( &puller->stat, sizeof (StatisticInfo) );
	epoll_ctl( puller->efd, EPOLL_CTL_ADD, puller->conn.fSocket, &puller->ev_r );
	while ( 0 != puller->conn.connect( SERVER_IP, SERVER_PORT ) )
	{
		printf( "Connect to server %s:%d failed.\n", SERVER_IP, SERVER_PORT );
		msleep( 1000 );
		continue;
	}
	printf( "Successful connected.\n" );
	std::thread reciver( thread_func_for_receiver, puller );
	std::thread writer( thread_func_for_writer, puller );
	std::thread controller( thread_func_for_controller, puller );

	reciver.join( );
	writer.join( );
	controller.join( );
	RTMP_LogThreadStop( );
	msleep( 10 );

	if ( puller )
		free( puller );

	if ( dumpfile )
		fclose( dumpfile );

	cleanup_sockets( );
#if WIN32
#ifdef _DEBUG
	_CrtDumpMemoryLeaks( );
#endif // _DEBUG
#endif
	return 0;
}