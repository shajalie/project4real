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
		if(logtail[i]->getLSN() < maxLSN) {
			se->updateLog(logtail[i]->toString());
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

	// done with begin checkpint?

	// end checkpoint

}

void LogMgr::commit(int txid) {

}

void LogMgr::pageFlushed(int page_id) {

}

void LogMgr::recover(string log) {

}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {

	// check point is where the writes go to the DB?
	// so update the log on this write?
	// int lsn = se.nextLSN();
	// int prevLSN = getLastLSN(txid);
	// logtail.push_back(new UpdateLogRecord(lsn, prevLSN, 
	// 	txid, page_id, offset, oldtxt, input));

	// // who knows
	// updatePage(page_id, offset, input);

}

// who knows
void LogMgr::setStorageEngine(StorageEngine* engine) {
	LogMgr::se = engine;
}





