/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"


/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MsgTypes{
    JOINREQ,
    JOINREP,
    HEARTBEAT,
    DUMMYLASTMSGTYPE
};

/**
 * STRUCT NAME: MessageHdr
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct MessageHdr {
	enum MsgTypes msgType;
}MessageHdr;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	
	char NULLADDR[6];
	bool isIntroducer;
	Queue msgQ;
	
public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
	int recvLoop();
	static int enqueueWrapper(void*, char*, int);
	void nodeStart(char*, short);
	int initThisNode(Address*);
	int introduceSelfToGroup(Address*);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void*, char*, int = 0);
	void nodeLoopOps();
	int isNullAddress(Address*);
	Address getJoinAddress();
	void initMemberListTable(Member*);
	void printAddress(Address*);
	virtual ~MP1Node();
private:
	int getNodeId(char []);
	int getNodePort(char []);
	long getHeartBeat(char*);
	void insertEntry(vector<MemberListEntry>&, Member*, int, short, long, long, bool = true);
	int sendMessage(Address*, char*, int);
	void getMemberList(vector<MemberListEntry>&, char*, bool = false);
	int getRandomReceivers(void);
	string getReceiverAddress(int, short);
	//binery search
	vector<MemberListEntry>::iterator getSelfPos(int low, int high);
	bool isNodeRemoved(int id, int port);
};

#endif /* _MP1NODE_H_ */
