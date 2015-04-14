// Sammy Hajalie
// Alie Hajalie

#include "LogMgr.h"
#include <sstream>
#include <limits>
#include <queue>
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
	//ACQUIRING MOST RECENT BEGIN_CHKPOINT LOG RECORD
	int mostRecentBegin = 0; //not 0?
	for(int i = 0; i < log.size(); ++i) {
		if(log[i]->getType() == BEGIN_CKPT) {
			mostRecentBegin = i;
		}
	}
	//Get associated END_CHKPOINT
	int endCheckIndex = mostRecentBegin;
	for(int i = mostRecentBegin; i < log.size(); ++i) {
		if(log[i]->getType() == END_CKPT) {
			ChkptLogRecord * end_ptr = dynamic_cast<ChkptLogRecord *>(log[i]);
			//INITIALIZE TABLES
			tx_table = end_ptr->getTxTable();
			dirty_page_table = end_ptr->getDirtyPageTable();
			break;

		}
	}
	//NEED TO GET PROPER LOG RECRODS, ETC
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
		//top if chec
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
	//Find smallest log record in dirt page table
	int min = std::numeric_limits<int>::max();
	for(auto& kv : dirty_page_table) {
		if(kv.second < min) {
			min = kv.second;
		}
	}
	//Find index for smallest log
	int index = 0;
	for(int i = 0; i < log.size(); ++i) {
		if(log[i]->getLSN() == min) {
			index = log[i]->getLSN();
			break;
		}
	}
	for(int i = index; i < log.size(); ++i) {
		if(log[i]->getType() == UPDATE) {
			UpdateLogRecord * upd_ptr = dynamic_cast<UpdateLogRecord *>(log[i]);
			se->pageWrite(upd_ptr->getPageID(), upd_ptr->getOffset(), 
				upd_ptr->getAfterImage(), upd_ptr->getLSN());
		}
		if(log[i]->getType() == CLR) {
			CompensationLogRecord * chk_ptr = dynamic_cast<CompensationLogRecord *>(log[i]);
			se->pageWrite(chk_ptr->getPageID(), chk_ptr->getOffset(),
				chk_ptr->getAfterImage(), chk_ptr->getLSN());
		}
	}
	//DONE

}

void LogMgr::undo(vector <LogRecord*> log, int txnum) {
	
}


vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
	vector<LogRecord*> result;
	istringstream stream(logstring);
	string line;
	while(getline(stream, line)) {
		LogRecord* lr = LogRecord::stringToRecordPtr(line);
		result.push_back(lr);
	}
	return result;
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

	//Begin Checkpoint
	logtail.push_back(new LogRecord(se->nextLSN(), NULL_LSN, NULL_TX, BEGIN_CKPT));

	//End Checkpoint
	logtail.push_back(new ChkptLogRecord(se->nextLSN(), NULL_LSN, 
		NULL_TX, tx_table, dirty_page_table));

}

void LogMgr::commit(int txid) {
	// write begin checkpoint to log file?
	// int lsn = se.nextLSN();
	// int prevLSN = getLastLSN(txid);
	// logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));

	int lsnBeginCkpt = se->nextLSN();
	LogRecord* lr = new LogRecord(lsnBeginCkpt, NULL_LSN, NULL_TX, BEGIN_CKPT);
	logtail.push_back(lr);

	// which transaction and which page for endcheckpoint
	// tx_table and dirty page
	int lsnEndChpt = se->nextLSN();
	LogRecord* endChkLog = new ChkptLogRecord(lsnEndChpt, 
		lsnBeginCkpt, NULL_TX, tx_table, dirty_page_table);
	logtail.push_back(endChkLog);

	// flush txTable and dirty page tablet to disk
	// which is the log
	flushLogTail(lsnEndChpt);
}

void LogMgr::pageFlushed(int page_id) {
	flushLogTail(page_id);
}

void LogMgr::recover(string log) {
	vector<LogRecord*> lr = stringToLRVector(log);
	analyze(lr);
	//MORE TO DO
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





