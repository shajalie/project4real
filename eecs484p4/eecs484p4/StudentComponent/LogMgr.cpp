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

int findLSNcheckpoint(vector <LogRecord*> log, int LSN) {
	for(int i = 0; i < log.size(); ++i) {
		if(log[i]->getLSN() == LSN) {
			return i;
		}
	}
	return 0;
}

void LogMgr::analyze(vector <LogRecord*> log) {
	//ACQUIRING MOST RECENT BEGIN_CHKPOINT LOG RECORD
	// int mostRecentBegin = 0; //not 0?
	// for(int i = 0; i < log.size(); ++i) {
	// 	if(log[i]->getType() == BEGIN_CKPT) {
	// 		mostRecentBegin = i;
	// 	}
	// }
	int mostRecentBegin = findLSNcheckpoint	(log, se->get_master());
	// if(mostRecentBegin == -1) {
	// 	mostRecentBegin = 0;
	// }
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
	for(int i = mostRecentBegin; i < log.size(); ++i) {
		if( log[i]->getType() == END) {
			//MAY NEED TO DO ADDITIONAL CHECKS
			tx_table.erase(log[i]->getTxID() );
		}
		else {
			//ADD TO TRANSACTION TABLE
			txTableEntry ab;
			tx_table[log[i]->getTxID() ] = ab;
			tx_table[log[i]->getTxID() ].lastLSN = log[i]->getLSN();
			// setLastLSN(log[i]->getTxID() , log[i]->getLSN());
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
			index = i;
			break;
		}
	}
	for(int i = index; i < log.size(); ++i) {
		if(log[i]->getType() == UPDATE) {
			UpdateLogRecord * upd_ptr = dynamic_cast<UpdateLogRecord *>(log[i]);
			bool toRedo = true;
			if(dirty_page_table.count(upd_ptr->getPageID()) == 0) {
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
			if(dirty_page_table.count(chk_ptr->getPageID()) == 0) {
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
			// setLastLSN(kv.first, lsn);
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
		for(auto& kv : tx_table) {
			toUndo.push(getLastLSN(kv.first));
		}
	}
	else {
		toUndo.push(getLastLSN(txnum));
	}
	/*Stores the list of transactions that have a CLR written for them,
	so afterwards end records can be written.*/
	// map<int, bool> transactionsCLR;
		//REMEMBER TO REMOVE LOG RECORD FROM TRANSACTION TABLE
	//ONLY WRITE END ON SUCCEED PAGEWRITE
	while(!toUndo.empty()) {
		LogRecord* lr = findLSN(log, toUndo.top());
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
				// setLastLSN(chk_ptr->getTxID(), newLSN);
			// setLastLSN(chk_ptr->getTxID(), NULL_LSN);
				tx_table.erase(chk_ptr->getTxID());
				toUndo.pop();
			}
		}
		else if(lr->getType() == UPDATE) {
			UpdateLogRecord * chk_ptr = dynamic_cast<
				UpdateLogRecord *>(lr);
			int newLSN = se->nextLSN();
			CompensationLogRecord* updLR = new CompensationLogRecord(newLSN,
				getLastLSN(chk_ptr->getTxID()), chk_ptr->getTxID(),
				 chk_ptr->getPageID(), chk_ptr->getOffset(),
				chk_ptr->getBeforeImage(), chk_ptr->getprevLSN());
			logtail.push_back(updLR);
			// log.push_back(updLR);
			setLastLSN(chk_ptr->getTxID(), newLSN);
			//getbefore or getafter?
			bool a = se->pageWrite(chk_ptr->getPageID(), chk_ptr->getOffset(), chk_ptr->getBeforeImage(), newLSN);
			if(a == false) {
				return;
			}
			/*This part not needed? (adding end record?)*/
			// newLSN = se->nextLSN();
			// logtail.push_back(new LogRecord(newLSN, getLastLSN(chk_ptr->getTxID()),
			// chk_ptr->getTxID(), END ));
			// tx_table.erase	(chk_ptr->getTxID());

			// setLastLSN(chk_ptr->getTxID(), newLSN);
			// setLastLSN(chk_ptr->getTxID(), -1);
			toUndo.pop();
			toUndo.push(chk_ptr->getprevLSN());
			if(chk_ptr->getprevLSN() == NULL_LSN) {
				/*Write end log*/
				newLSN = se->nextLSN();
				logtail.push_back(new LogRecord(newLSN, getLastLSN(chk_ptr->getTxID()),
				chk_ptr->getTxID(), END ));
				tx_table.erase	(chk_ptr->getTxID());
			}
			else {
				// LogRecord* temp = 
				/*Chance of segmentation fault here maybe?*/
				if(findLSN(log, chk_ptr->getprevLSN())->getType() != UPDATE) {
					/*Write end log*/
					newLSN = se->nextLSN();
					logtail.push_back(new LogRecord(newLSN, getLastLSN(chk_ptr->getTxID()),
					chk_ptr->getTxID(), END ));
					tx_table.erase	(chk_ptr->getTxID());
				}
			}
		}
		else if(lr->getType() == ABORT) {
			toUndo.pop();
			toUndo.push(lr->getprevLSN());
		}
		else {
			toUndo.pop();
			toUndo.push(lr->getprevLSN());
			
		}
	}
	/*
	Any transactions still left must be because of written CLRs.
	Now we find the larg */
	//MODIFY DIRTY PAGE TABLE
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
	LogRecord* lr = new LogRecord(lsn, getLastLSN(txid), txid, ABORT);
	logtail.push_back(lr);
	setLastLSN(txid, lsn);
	// flushLogTail(lsn);
	vector<LogRecord*> logStored = stringToLRVector(se->getLog());
	vector<LogRecord*> log;
	log.reserve(logStored.size() + logtail.size());
	log.insert(log.end(), logStored.begin(), logStored.end());
	log.insert(log.end(), logtail.begin(), logtail.end());
	// analyze(log);
	// redo(log);
	// txTableEntry a = txTableEntry(lsn, U);
	// tx_table[txid] = a;
	// analyze(log);
	// redo(log);
	undo(log, txid);
	//add end log
	/*Commenting this part out because it should already be done in the undo phase*/
	// lsn = se->nextLSN();
	// logtail.push_back(new LogRecord(lsn, getLastLSN(txid), 
	// 	txid, END));
	// tx_table.erase(txid);

	// setLastLSN(txid, lsn);
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
	//End Checkpoint
	int lsn = se->nextLSN();
	logtail.push_back(new ChkptLogRecord(lsn, bLSN, 
		NULL_TX, tx_table, dirty_page_table));
	//When you've added a checkpoint to the 
	//logtail, you should flush the log tail to disk and write the master record; see page 587.
	flushLogTail(lsn);
	se->store_master(bLSN);


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


	
	// // make it c
	// txTableEntry tempUpdate(lsn, C);
	// tx_table[txid] = tempUpdate; // or use map.insert
	flushLogTail(lsn);

	//MUST DELETE FROM TX TABLE WHEN COMMIT
	lsn = se->nextLSN();
	logtail.push_back(new LogRecord(lsn, getLastLSN(txid), 
		txid, END));
	setLastLSN(txid, lsn);
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
	// flushLogTail(lsn);

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
	dirty_page_table[page_id] = lsn;

	// use updateLog in se, piazza
	string logString = lr->toString();
	// se->updateLog(logString);
	return lsn;

}

// who knows
void LogMgr::setStorageEngine(StorageEngine* engine) {
	LogMgr::se = engine;
}





