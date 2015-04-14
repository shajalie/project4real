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
	for(int i = 0; i < log.size(); ++i) {
		if( log[i]->getType() == END) {
			//MAY NEED TO DO ADDITIONAL CHECKS
			tx_table.erase(log[i]->getTxID() );
		}
		else {
			//ADD TO TRANSACTION TABLE
			tx_table[log[i]->getTxID() ].lastLSN = log[i]->getLSN();
			if(log[i]->getType() == COMMIT) {
				tx_table[log[i]->getTxID() ].status = C;
			}
			else {
				tx_table[log[i]->getTxID() ].status = U;
			}
		}
		if( (log[i]->getType() == UPDATE || log[i]->getType() == CLR) ) {
			if(log[i]->getType() == UPDATE) {
				UpdateLogRecord * upd_ptr = dynamic_cast<UpdateLogRecord *>(log[i]);
				if(dirty_page_table.count(upd_ptr->getPageID()) == 0) {
					dirty_page_table[upd_ptr->getPageID()] = upd_ptr->getLSN();
				}
			}
			if(log[i]->getType() == CLR) {
				CompensationLogRecord * chk_ptr = dynamic_cast<CompensationLogRecord *>(log[i]);
				if(dirty_page_table.count(chk_ptr->getPageID()) == 0) {
					dirty_page_table[chk_ptr->getPageID()] = chk_ptr->getLSN();
				}
			}
		}
	}
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





