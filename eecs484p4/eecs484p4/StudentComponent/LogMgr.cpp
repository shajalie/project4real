// Sammy Hajalie
// Alie Hajalie

#include "LogMgr.h"


int LogNgr::getLastLSN(int txnum) {
	// not sure, get the lastest that is in the logtail
	// if not get the lastest one in the log file?
	if(tx_table.count(txnum) > 0) {
		return tx_table[txnum].lastSLN;
	}
	else {
		return null;
	}
	//test
}

void LogMgr::setLastLSN(int txnum, int lsn) {

	// set Prev Lsn of this tx to last lsn

	// set the new lastLSN
	tx_table[txnum].lastLSN = lsn;

}

// who knows
void LogMgr::setStorageEngine(StorageEngine* engine) {
	se = engine;
}

void LogMgr::checkpoint() {
	// write begin checkpoint to log file?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));
	// done with begin checkpint?


	// end checkpoint

}

void LogMgr::commit(int txid) {
	// change the status of tx_id from U to C
	// update last lsn to the current LSN
	// write to logtail?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	if (!prevLSN) {
		prevLSN = -1;
	}

}

void LogMgr::analyze(vector <LogRecord*> log) {


}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {

	// check point is where the writes go to the DB?
	// so update the log on this write?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	if (!prevLSN) {
		prevLSN = -1; // was null
	}

	LogRecord* lr = new UpdateLogRecord(lsn, prevLSN, 
		txid, page_id, offset, oldtxt, input);
	logtail.push_back(lr));
	// add this to the transaction table
	// make it U
	txTableEntry tempUpdate(lsn, U);
	tx_table[txid] = tempUpdate; // or use map.insert

	// use updateLog in se, piazza
	string logString = lr->toString();
	se->updateLog(logString);


	// storage engine expects to get back page lsn for the write?
	se.updateLSN(page_id, lsn);

	// who knows
	updatePage(page_id, offset, input);

}