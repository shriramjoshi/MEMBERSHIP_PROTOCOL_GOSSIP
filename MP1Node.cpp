

/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream>
#include <set>
/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */


/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->isIntroducer = false;
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	//this->removed_nodes = (int*)calloc(par->EN_GPSZ, sizeof(int));
	srand(time(NULL));
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {
	
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( !memberNode->bFailed ) {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
    return false;
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
 	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
	this->initMemberListTable(memberNode);
	return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        isIntroducer = true;
        log->logNodeAdd(&memberNode->addr, &memberNode->addr);
        this->insertEntry(memberNode->memberList, memberNode, this->getNodeId(memberNode->addr.addr), 
        this->getNodePort(memberNode->addr.addr), memberNode->heartbeat, par->getcurrtime());
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
		
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...with heartbeat %ld", memberNode->heartbeat);
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
		
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	//TODO
   /*
    * Your code goes here
    */
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }
	
	
	// Check my messages
    checkMessages();
	
    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }
    
	memberNode->myPos = getSelfPos(0, memberNode->memberList.size() - 1);
	
	if(memberNode->myPos != memberNode->memberList.end()) {
		(*memberNode->myPos).heartbeat += 1;
		(*memberNode->myPos).timestamp = par->getcurrtime();
	}
    
    nodeLoopOps();
	
    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;
	
	
    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	MessageHdr* hdr = (MessageHdr *)(data);
	Member* memberNode = (Member*)env;
	if(hdr->msgType == JOINREQ) {
		Address to;
		memcpy(&to.addr, (char*)(data + sizeof(MessageHdr)), sizeof(memberNode->addr.addr));
		// create JOINREP message: format of data is {MessageHdr , Address.addr, id, port, heartbeat, timestamp
		size_t msgsize = sizeof(MessageHdr) + 
						sizeof(int)	+																		//size of member list
						sizeof(memberNode->addr.addr/* sending node address */) +							//from addr
						(sizeof(int) + sizeof(short) + sizeof(long) * 2) * memberNode->memberList.size();   //member list entries
		MessageHdr* msg = (MessageHdr *) calloc(msgsize, sizeof(char));
		msg->msgType = JOINREP;
	    memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	    int nodes = (int)memberNode->memberList.size();
	    memcpy((char* )(msg + 1) + sizeof(memberNode->addr.addr), &nodes, sizeof(int));
	    this->sendMessage(&to, (char *)msg, msgsize);
	    this->insertEntry(memberNode->memberList, memberNode, this->getNodeId(to.addr), this->getNodePort(to.addr), memberNode->heartbeat, par->getcurrtime());
	    memset(msg, 0, msgsize);
	    free(msg);
		
	}
	else if(hdr->msgType == JOINREP) {
		getMemberList(memberNode->memberList, data, true);
		memberNode->inGroup = true;
	}
	else if(hdr->msgType == HEARTBEAT) {
		vector<MemberListEntry> memberList;
		getMemberList(memberList, data);
		vector<MemberListEntry>::iterator it_beg = memberList.begin();
		vector<MemberListEntry>::iterator it_end = memberList.end();
		for(; it_beg != it_end; ++it_beg) {
			this->insertEntry(memberNode->memberList, memberNode, (*it_beg).id, (*it_beg).port, (*it_beg).heartbeat, (*it_beg).timestamp);
		}
	}
	return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	set<int> vec_receivers;
	std::set<int>::iterator it;
  	std::pair<std::set<int>::iterator,bool> ret;
	int rand_receivers = getRandomReceivers();
	for(int i = 0; i < rand_receivers; ++i) {
		int rand_rcvr = rand() % memberNode->memberList.size();
		if(memberNode->memberList[rand_rcvr].id > 0) {
			ret = vec_receivers.insert(rand_rcvr);
			//No receiver gets heartbeat twice
			if(ret.second != false) {
				MemberListEntry receiver = memberNode->memberList.at(rand_rcvr);
				Address to(getReceiverAddress(receiver.getid(), receiver.getport()));
				//No self messaging
				if(memcmp(&to.addr, &memberNode->addr.addr, sizeof(to.addr)) != 0) { 
					size_t msgsize = sizeof(MessageHdr) + 
								sizeof(int)	+																		//size of member list
								sizeof(memberNode->addr.addr/* sending node address */) +							//from addr
								(sizeof(int) + sizeof(short) + sizeof(long) * 2) * memberNode->memberList.size();   //member list entries
					MessageHdr* msg = (MessageHdr *) calloc(msgsize, sizeof(char));
					msg->msgType = HEARTBEAT;
					memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
					int nodes = (int)memberNode->memberList.size();
					memcpy((char* )(msg + 1) + sizeof(memberNode->addr.addr), &nodes, sizeof(int));
					this->sendMessage(&to, (char *)msg, msgsize);
				}
			}
		}
	}
	
	//First check if any removable node
	vector<MemberListEntry>::iterator it_beg = memberNode->memberList.begin();
	vector<MemberListEntry>::iterator it_end = memberNode->memberList.end();
	for(; it_beg != it_end; ++it_beg) {
		MemberListEntry entry = *it_beg;
		if(entry.id > 0) {
			if(memberNode->myPos != it_beg) {
				long curtime = par->getcurrtime();
				if((curtime - entry.timestamp) > (TREMOVE + TFAIL)) {
					Address rem_address(getReceiverAddress(entry.id, entry.port));
					(*it_beg).id *= -1;
					(*it_beg).port *= -1;
					log->logNodeRemove(&memberNode->addr, &rem_address);
				}
			}
		}
	}
	return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member* memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address* addr){
	printf("%d.%d.%d.%d:%d",  addr->addr[0],addr->addr[1],addr->addr[2], addr->addr[3], *(short*)&addr->addr[4]);  
}


///////////////////////////// Private Member Functions ////////////////////////////////////////////////////

int MP1Node::getNodeId(char addr[]) {
	return (int)addr[0];
}

int MP1Node::getNodePort(char addr[]) {
	return (int)addr[4];
}

long MP1Node::getHeartBeat(char* data) {
	long heartbeat = 0l;
	memcpy(&heartbeat, (data + 1 + sizeof(memberNode->addr.addr))/* heartbeat */, sizeof(long));
	return heartbeat;
}

/**
 * FUNCTION NAME: insertEntry
 *
 * DESCRIPTION: Creates member list entry & inserts into vector
*/
void MP1Node::insertEntry(vector<MemberListEntry>& memberList, Member* memberNode, int id, short port, long heartbeat, long timestamp, bool flag) {
	vector<MemberListEntry>::iterator it_beg = memberList.begin();
	vector<MemberListEntry>::iterator it_end = memberList.end();
	MemberListEntry entry(id, port, heartbeat, timestamp);
	//If nodes has been deleted then ignore this entry
	Address addedNode(getReceiverAddress(id, port));
	if(isNodeRemoved(id, port)) {
		return;
	}
	if(it_beg == it_end) {
		entry.timestamp = par->getcurrtime();
		memberList.push_back(entry);
		if(flag) {
			log->logNodeAdd(&memberNode->addr, &addedNode);
		}
	}
	else {
		for(; it_beg != it_end; ++it_beg) {
			vector<MemberListEntry>::iterator it = it_beg;
			//if entry is less insert befor
			if((entry.id + entry.port) < ((*it).id + (*it).port)) {
				entry.timestamp = par->getcurrtime();
				memberList.insert(it, entry);
				if(flag) {
					log->logNodeAdd(&memberNode->addr, &addedNode);
				}
				return;
			}
			//if entry already exists check if
			//heartbeat is higher than erase old entry & add new entry
			else if((entry.id + entry.port) == ((*it).id + (*it).port)) {
				if(entry.heartbeat > (*it).heartbeat) {
					(*it).timestamp = par->getcurrtime();
					(*it).heartbeat = entry.heartbeat;
				}
				return;
			}
		}
		entry.timestamp = par->getcurrtime();
		memberList.push_back(entry);
		if(flag) {
			log->logNodeAdd(&memberNode->addr, &addedNode);
		}
	}
	return;
}

int MP1Node::sendMessage(Address* to, char* msg, int size) {
	char* ptr = msg + sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(int);
	vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
	for(; it != memberNode->memberList.end(); ++it) {
		
		if((*it).id > 0) {
			MemberListEntry entry((*it).id, (*it).port, (*it).heartbeat, (*it).timestamp);
			memcpy((char* )(ptr), &entry.id, sizeof(int));														//NODE id
			memcpy((char* )(ptr + sizeof(int)), &entry.port, sizeof(short));									//NODE port
			memcpy((char* )(ptr + sizeof(int) + sizeof(short)), &entry.heartbeat, sizeof(long));				//NODE heartbeat
			memcpy((char* )(ptr + sizeof(int) + sizeof(short) + sizeof(long)), &entry.timestamp, sizeof(long));	//NODE local-timestamp
			ptr += sizeof(int) + sizeof(short) + 2 * sizeof(long);
		}
	}
	return emulNet->ENsend(&memberNode->addr, to, msg, size);
}

void MP1Node::getMemberList(vector<MemberListEntry>& memberList, char* data, bool flag) {
	char* ptr = data + sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(int);
	int size = 0;
	memcpy(&size, data + sizeof(MessageHdr) + sizeof(memberNode->addr.addr), sizeof(int));
	for(int i = 0; i < size; ++i) {
		MemberListEntry entry(0, 0, 0l, 0l);
		memcpy(&entry.id, (int*)(ptr), sizeof(int));
		memcpy(&entry.port, (short*)(ptr + sizeof(int)), sizeof(short));
		memcpy(&entry.heartbeat, (long*)(ptr + sizeof(int) + sizeof(short)), sizeof(long));
		memcpy(&entry.timestamp, (long*)(ptr + sizeof(int) + sizeof(short) + sizeof(long)), sizeof(long));
		if(entry.id > 0) {
			if(flag)
				this->insertEntry(memberList, memberNode, entry.id, entry.port, entry.heartbeat, entry.timestamp, true);
			else 
				this->insertEntry(memberList, memberNode, entry.id, entry.port, entry.heartbeat, entry.timestamp, false);
		}
		ptr += sizeof(int) + sizeof(short) + 2 * sizeof(long);
	}
	

	return;
}

int MP1Node::getRandomReceivers(void) {
	int nodes = memberNode->memberList.size();
	int rand_receivers = rand() % nodes;
	return rand_receivers;
}

string MP1Node::getReceiverAddress(int id, short port) {
	string address = ".0.0.0:";
	std::stringstream ss_id, ss_port;
	ss_id << id;
	ss_port << port;
	address = ss_id.str() + address;
	address += ss_port.str();
	return address;
}

vector<MemberListEntry>::iterator MP1Node::getSelfPos(int low, int high) {
	vector<MemberListEntry>::iterator it_beg = memberNode->memberList.begin();
	vector<MemberListEntry>::iterator it_end = memberNode->memberList.end();
	for(; it_beg != it_end; ++it_beg) {
		if(getNodeId(memberNode->addr.addr) == (*it_beg).id) {
			return it_beg;
		}
	}
	return it_end;
	/*if(low > high) {
		return memberNode->memberList.end();
	}
	else {
		int mid = low + (high - low)/2;
		vector<MemberListEntry>::iterator it = memberNode->memberList.begin() + mid;
		if(getNodeId(memberNode->addr.addr) == (*it).id) {
			return it;
		}
		else if(getNodeId(memberNode->addr.addr) < (*it).id) {
			return getSelfPos(low, mid);
		}
		else {
		 	return getSelfPos(mid + 1, high);
		}
	}*/
}

bool MP1Node::isNodeRemoved(int id, int port) {
	std::vector<MemberListEntry>:: iterator it_beg = memberNode->memberList.begin();
	std::vector<MemberListEntry>:: iterator it_end = memberNode->memberList.end();
	for(; it_beg != it_end; ++it_beg) {
		MemberListEntry entry = *it_beg;
		if(id == (entry.id * -1)) {
			return true;
		}
	}
	return false;
}

