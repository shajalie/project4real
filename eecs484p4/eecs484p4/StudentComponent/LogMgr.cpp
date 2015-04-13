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
	//testas
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

void LogMgr::analyze(vector <LogRecord*> log) {


}

int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {

	// check point is where the writes go to the DB?
	// so update the log on this write?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	logtail.push_back(new UpdateLogRecord(lsn, prevLSN, 
		txid, page_id, offset, oldtxt, input));

	// who knows
	updatePage(page_id, offset, input);

}