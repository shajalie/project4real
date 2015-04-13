// Sammy Hajalie

#include "LogMgr.h"

void 

int getLastLSN(int txnum) {
	// not sure, get the lastest that is in the logtail
	// if not get the lastest one in the log file?
	if (logtail.size() != 0) {
		for (int = logtail.size()-1 ; i >= 0 ; i--) {
			LogRecord *temp = logtail[i];
			if (temp->getTxID() == txnum) {
				return temp->getLSN();
			}
		}
	}
}

void setLastLSN(int txnum, int lsn) {

	// set Prev Lsn of this tx to last lsn

	// set the new lastLSN
	tx_table[txnum].lastLSN = lsn;

}

// who knows
void setStorageEngine(StorageEngine* engine) {
	se = engine;
}

void checkpoint() {
	// write begin checkpoint to log file?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));

	// done with begin checkpint?

	// end checkpoint

}

void analyze(vector <LogRecord*> log) {


}

int write(int txid, int page_id, int offset, string input, string oldtext) {

	// check point is where the writes go to the DB?
	// so update the log on this write?
	int lsn = se.nextLSN();
	int prevLSN = getLastLSN(txid);
	logtail.push_back(new UpdateLogRecord(lsn, prevLSN, 
		txid, page_id, offset, oldtxt, input));

	// who knows
	updatePage(page_id, offset, input);

}