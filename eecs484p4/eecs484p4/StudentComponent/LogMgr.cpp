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
	// int mostRecentBegin = 0; //not 0?
	// for(int i = 0; i < log.size(); ++i) {
	// 	if(log[i]->getType() == BEGIN_CKPT) {
	// 		mostRecentBegin = i;
	// 	}
	// }
	int mostRecentBegin = se->get_master();
	//Get associated END_CHKPOINT
	int endCheckIndex = mostRecentBegin;
	for(int i = mostRecentBegin; i < log.size(); ++i) {
		if(log[i]->getType() == END_CKPT) {
			ChkptLogRecord * end_ptr = dynamic_cast<ChkptLogRecord *>(log[i]);
			endCheckIndex = i;
			//INITIALIZE TABLES
			tx_table = end_ptr->getTxTable();
			dirty_page_table = end_ptr->getDirtyPageTable();
			break;

		}
	}
	//NEED TO GET PROPER LOG RECRODS, ETC
	for(int i = endCheckIndex; i < log.size(); ++i) {
		if( log[i]->getType() == END) {
			//MAY NEED TO DO ADDITIONAL CHECKS
			tx_table.erase(log[i]->getTxID() );
		}
		else {
			//ADD TO TRANSACTION TABLE
			txTableEntry ab = txTableEntry();
			tx_table[log[i]->getTxID() ] = ab;
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
			bool toRedo = true;
			if(dirty_page_table.count(upd_ptr->getPageID()) > 0) {
				toRedo = false;
			}
			else if(dirty_page_table[upd_ptr->getPageID()] > upd_ptr->getLSN()) {
				toRedo = false;
			}
			else if(se->getLSN(upd_ptr->getPageID()) >= upd_ptr->getLSN()) {
				toRedo = false;
			} 
			if(toRedo) {
				bool a = se->pageWrite(upd_ptr->getPageID(), upd_ptr->getOffset(), 
				upd_ptr->getAfterImage(), upd_ptr->getLSN());
				if(a == false) {
					return false;
				}
			}
		}
		if(log[i]->getType() == CLR) {
			CompensationLogRecord * chk_ptr = dynamic_cast<CompensationLogRecord *>(log[i]);
			bool toRedo = true;
			if(dirty_page_table.count(chk_ptr->getPageID()) > 0) {
				toRedo = false;
			}
			else if(dirty_page_table[chk_ptr->getPageID()] > chk_ptr->getLSN()) {
				toRedo = false;
			}
			else if(se->getLSN(chk_ptr->getPageID()) >= chk_ptr->getLSN()) {
				toRedo = false;
			} 
			if(toRedo) {
				bool a = se->pageWrite(chk_ptr->getPageID(), chk_ptr->getOffset(),
					chk_ptr->getAfterImage(), chk_ptr->getLSN());
				if(a == false) {
					return false;
				}
			}
		}
	}
	for(auto& kv : tx_table) {
		if(kv.second.status == C) {
			int lsn = se->nextLSN();
			logtail.push_back(new LogRecord(lsn, getLastLSN(kv.first),
				kv.first, END));
			tx_table.erase(kv.first);
		}
	}
	return true;
	//DONE

}

LogRecord* findLSN(vector <LogRecord*> log, int LSN) {
	for(int i = 0; i < log.size(); ++i) {
		if(log[i]->getLSN() == LSN) {
			return log[i];
		}
	}
	return NULL;
}

void LogMgr::undo(vector <LogRecord*> log, int txnum) {
	priority_queue<int> toUndo;
	if(txnum == NULL_TX) {
		for(auto& kv : dirty_page_table) {
			toUndo.push(kv.second);
		}
	}
	else {
		toUndo.push(txnum);
	}
	
		//REMEMBER TO REMOVE LOG RECORD FROM TRANSACTION TABLE
	while(!toUndo.empty()) {
		LogRecord* lr = findLSN(log, getLastLSN(toUndo.top()));
		if(lr == NULL) {
			toUndo.pop();
			continue;
		}
		if(lr->getType() == CLR) {
			CompensationLogRecord * chk_ptr = dynamic_cast<
				CompensationLogRecord *>(lr);
			if(chk_ptr->getUndoNextLSN() != NULL_LSN) {
				toUndo.pop();
				toUndo.push(chk_ptr->getUndoNextLSN());
			}
			else {
				int newLSN = se->nextLSN();
				logtail.push_back(new LogRecord(newLSN, getLastLSN(chk_ptr->getTxID()),
				 chk_ptr->getTxID(), END));
				setLastLSN(chk_ptr->getTxID(), newLSN);
				tx_table.erase(toUndo.top());
				toUndo.pop();
			}
		}
		else if(lr->getType() == UPDATE) {
			UpdateLogRecord * chk_ptr = dynamic_cast<
				UpdateLogRecord *>(lr);
			int newLSN = se->nextLSN();
			logtail.push_back(new CompensationLogRecord(newLSN,
				getLastLSN(chk_ptr->getTxID()), chk_ptr->getTxID(),
				 chk_ptr->getPageID(), chk_ptr->getOffset(),
				chk_ptr->getBeforeImage(), chk_ptr->getprevLSN()));
			setLastLSN(chk_ptr->getTxID(), newLSN);
			se->pageWrite(chk_ptr->getPageID(), chk_ptr->getOffset(), chk_ptr->getAfterImage(), newLSN);
			newLSN = se->nextLSN();
			logtail.push_back(new LogRecord(newLSN, getLastLSN(chk_ptr->getTxID()),
			chk_ptr->getTxID(), END ));
			setLastLSN(chk_ptr->getTxID(), newLSN);
			toUndo.pop();
			toUndo.push(chk_ptr->getprevLSN());
		}
		else {
			toUndo.pop();
		}
	}
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
	int lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), txid, ABORT));
	setLastLSN(txid, lsn);
	undo(logtail, txid);
	//add end log
	lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), 
		txid, END));
	setLastLSN(txid, lsn);
}

void LogMgr::checkpoint() {
	// write begin checkpoint to log file?
	// int lsn = se.nextLSN();
	// int prevLSN = getLastLSN(txid);
	// logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));

	// done with begin checkpint?

	// end checkpoint

	//Begin Checkpoint
	int bLSN = se->nextLSN();
	logtail.push_back(new LogRecord(bLSN, NULL_LSN, NULL_TX, BEGIN_CKPT));
	se->store_master(bLSN);
	//End Checkpoint
	int lsn = se->nextLSN();
	logtail.push_back(new ChkptLogRecord(lsn, NULL_LSN, 
		NULL_TX, tx_table, dirty_page_table));
	flushLogTail(lsn);

}

void LogMgr::commit(int txid) {
	// write begin checkpoint to log file?
	// int lsn = se.nextLSN();
	// int prevLSN = getLastLSN(txid);
	// logtail.push_back(new LogRecord(lsn, prevLSN, BEGIN_CKPT));

	int lsn = se->nextLSN();
	int prevLSN = getLastLSN(txid);
	if (!prevLSN) {
		prevLSN = NULL_LSN;
	}
	LogRecord* lr = new LogRecord(lsn, prevLSN, txid, COMMIT);
	setLastLSN(txid, lsn);
	logtail.push_back(lr);
	lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), 
		txid, END));
	setLastLSN(txid, lsn);
	// // make it c
	// txTableEntry tempUpdate(lsn, C);
	// tx_table[txid] = tempUpdate; // or use map.insert
	flushLogTail(lsn);
	//MUST DELETE FROM TX TABLE WHEN COMMIT
	tx_table.erase(txid);

}

void LogMgr::pageFlushed(int page_id) {
	flushLogTail(se->getLSN(page_id));
	dirty_page_table.erase(page_id);
}

void LogMgr::recover(string log) {
	vector<LogRecord*> lr = stringToLRVector(log);
	analyze(lr);
	bool a = redo(lr);
	if(a == false) {
		//PIAZZA says to drop everything if returns false
		return;
	}
	undo(lr, NULL_TX);
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
	setLastLSN(txid, lsn);
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





