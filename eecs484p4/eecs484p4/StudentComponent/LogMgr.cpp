// Sammy Hajalie
// Alie Hajalie

#include "LogMgr.h"

using namespace std;

int LogMgr::getLastLSN(int txnum) {
	// not sure, get the lastest that is in the logtail
	// if not get the lastest one in the log file?
	if(tx_table.count(txnum) > 0) {
		return tx_table[txnum].lastLSN;
	}
	else {
		return NULL_TX;
	}
	//testasassa
}

void LogMgr::setLastLSN(int txnum, int lsn) {

	// set Prev Lsn of this tx to last lsn

	// set the new lastLSN
	tx_table[txnum].lastLSN = lsn;

}

void LogMgr::flushLogTail(int maxLSN) {
	for(int i = 0; i < logtail.size(); ++i) {
		if(logtail[i]->getLSN() <= maxLSN) {
			se->updateLog(logtail[i]->toString());
			delete logtail[i];
			logtail.erase(logtail.begin() + i);
			i--;
		}
	}
}

void LogMgr::analyze(vector <LogRecord*> log) {


}

bool LogMgr::redo(vector <LogRecord*> log) {

}

void LogMgr::undo(vector <LogRecord*> log, int txnum) {
	
}

void LogMgr::abort(int txid) {

}

void LogMgr::checkpoint() {
	// write begin checkpoint to log file?
	// int lsn = se.nextLSN();
	// int prevLSN = getLastLSN(txid);
	// logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));

	int lsnBeginCkpt = se->nextLSN();
	LogRecord* lr = new LogRecord(lsnBeginCkpt, NULL_LSN, NULL_TX, BEGIN_CKPT);
	logtail.push_back(lr);

	// which transaction and which page for endcheckpoint
	// tx_table and dirty page
	int lsnEndChpt = se->nextLSN;
	LogRecord* endChkLog = new ChkptLogRecord(lsnEndChpt, 
		lsnBeginCkpt, NULL_TX, tx_table, dirty_page_table);
	logtail.push_back(endChkLog);

	// flush txTable and dirty page tablet to disk
	// which is the log
	flushLogTail(logtail);

}

void LogMgr::commit(int txid) {
	// change the status of tx_id from U to C
	// update last lsn to the current LSN
	// write to logtail?
	int lsn = se->nextLSN();
	int prevLSN = getLastLSN(txid);
	if (!prevLSN) {
		prevLSN = NULL_LSN;
	}
	LogRecord* lr = new LogRecord(lsn, prevLSN, txid, COMMIT);
	logtail.push_back(lr);

	// make it c
	txTableEntry tempUpdate(lsn, C);
	tx_table[txid] = tempUpdate; // or use map.insert
	flushLogTail(lsn);

}

void LogMgr::pageFlushed(int page_id) {

}

void LogMgr::recover(string log) {

}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {

	// check point is where the writes go to the DB?
	// so update the log on this write?
	int lsn = se->nextLSN();
	int prevLSN = getLastLSN(txid);
	if (!prevLSN) {
		prevLSN = NULL_LSN; // was null
	}

	LogRecord* lr = new UpdateLogRecord(lsn, prevLSN, 
		txid, page_id, offset, oldtext, input);
	logtail.push_back(lr);
	// add this to the transaction table
	// make it U
	txTableEntry tempUpdate(lsn, U);
	tx_table[txid] = tempUpdate; // or use map.insert

	// add to dirty page table, add earilest lsn for this page ID
	if (dirty_page_table.find(page_id) == end) {
		dirty_page_table[page_id] = lsn; // not found, so new one
	}


	// use updateLog in se, piazza
	string logString = lr->toString();
	// se->updateLog(logString);
	flushLogTail(lsn);
	return lsn;

}

// who knows
void LogMgr::setStorageEngine(StorageEngine* engine) {
	LogMgr::se = engine;
}




