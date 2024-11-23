#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File*       file;
    Status      status;
    FileHdrPage*    hdrPage;
    int         hdrPageNo;
    int         newPageNo;
    Page*       newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status == OK)
    {
        // File already exists
        return FILEEXISTS;
    }
    else
    {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.

        // Create the file
        status = db.createFile(fileName);
        if (status != OK) {
            return status;
        }

        // Open the newly created file
        status = db.openFile(fileName, file);
        if (status != OK) {
            return status;
        }

        // Allocate the header page
        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        if (status != OK) {
            db.closeFile(file);
            return status;
        }

        // Initialize the header page
        hdrPage = (FileHdrPage*) newPage;
        memset(hdrPage, 0, sizeof(FileHdrPage));
        strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);

        // Allocate the first data page
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            bufMgr->unPinPage(file, hdrPageNo, true);
            db.closeFile(file);
            return status;
        }

        // Initialize the data page and link it to the header page
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1); // No next page
        if (status != OK) {
            bufMgr->unPinPage(file, hdrPageNo, true);
            bufMgr->unPinPage(file, newPageNo, true);
            db.closeFile(file);
            return status;
        }

        hdrPage->recCnt = 0;
        hdrPage->pageCnt = 1;
        hdrPage->firstPage = hdrPage->lastPage = newPageNo;

        // Unpin the pages and mark them as dirty
        bufMgr->unPinPage(file, hdrPageNo, true);
        bufMgr->unPinPage(file, newPageNo, true);

        // Flush buffer pool before closing the file
        status = bufMgr->flushFile(file);
        if (status != OK) {
            cerr << "Error flushing buffer pool during createHeapFile" << endl;
            db.closeFile(file);
            return status;
        }

        // Close the file
        db.closeFile(file);
        return OK;
    }
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status  status;
    Page*   pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        returnStatus = OK;

        // Get the header page
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK) {
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }

        // Read the header page into the buffer pool
        status = bufMgr->readPage(filePtr, headerPageNo, (Page*&) headerPage);
        if (status != OK) {
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }
        hdrDirtyFlag = false;

        // Read the first data page into the buffer pool
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        curRec = NULLRID;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }

     // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, unpin current page
// and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record based on the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    // Check if the record is on the current page
    if (curPageNo != rid.pageNo) {
        // If there is a pinned page, unpin it
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                curPage = NULL;
                curPageNo = 0;
                curDirtyFlag = false;
                return status;
            }
        }

        // Read the target page into the buffer pool
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK) {
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }

    // Retrieve the record from the current page
    status = curPage->getRecord(rid, rec);
    if (status == OK) {
        curRec = rid;
    }
    return status;
}

HeapFileScan::HeapFileScan(const string & name,
               Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                     const int length_,
                     const Datatype type_,
                     const char* filter_,
                     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status  status = OK;
    RID     nextRid;
    RID     tmpRid;
    int     nextPageNo;
    Record      rec;

    if (curPageNo < 0)
        return FILEEOF; // Already at EOF!

    // Special case of the first record of the first page of the file
    if (curPage == NULL) {
        // Need to get the first page of the file
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1)
            return FILEEOF; // File is empty

        // Read the first page of the file
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
        curRec = NULLRID;

        // Get the first record off the page
        status = curPage->firstRecord(curRec);
        if (status == NORECORDS) {
            // Unpin the current page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPageNo = -1; // In case called again
            curPage = NULL; // For endScan()
            return FILEEOF; // First page had no records
        }

        // Get the record
        status = curPage->getRecord(curRec, rec);
        if (status != OK)
            return status;

        // See if record matches predicate
        if (matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }

    // When a page has been pinned
    while (true) {
        // Get the next record on the current page
        status = curPage->nextRecord(curRec, nextRid);
        if (status == OK) {
            curRec = nextRid;

            // Get the record
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
                return status;

            // See if record matches predicate
            if (matchRec(rec)) {
                outRid = curRec;
                return OK;
            }
        } else if (status == ENDOFPAGE || status == NORECORDS) {
            // Get the next page in the file
            status = curPage->getNextPage(nextPageNo);
            if (status != OK)
                return status;
            if (nextPageNo == -1)
                return FILEEOF; // End of file

            // Unpin the current page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = NULL;

            // Read the next page
            curPageNo = nextPageNo;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;
            curDirtyFlag = false;
            curRec = NULLRID;

            // Get the first record off the page
            status = curPage->firstRecord(curRec);
            if (status == NORECORDS)
                continue; // Page has no records, continue to next page

            // Get the record
            status = curPage->getRecord(curRec, rec);
            if (status != OK)
                return status;

            // See if record matches predicate
            if (matchRec(rec)) {
                outRid = curRec;
                return OK;
            }
        } else {
            return status;
        }
    }
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete the record from file
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}


// mark current page of scan as dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    if ((offset + length) > rec.length)
        return false;

    float diff = 0;
    switch(type) {

    case INTEGER:
        int iattr, ifltr;
        memcpy(&iattr, (char *)rec.data + offset, sizeof(int));
        memcpy(&ifltr, filter, sizeof(int));
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;
        memcpy(&fattr, (char *)rec.data + offset, sizeof(float));
        memcpy(&ffltr, filter, sizeof(float));
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset, filter, length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  // Do nothing. HeapFile constructor will read the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*   newPage;
    int     newPageNo;
    Status  status, unpinstatus;
    RID     rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        return INVALIDRECLEN;
    }

    if (curPage == NULL) {
        // Make the last page the current page and read it from disk
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false;
    }

    // Try to add the record onto the current page
    status = curPage->insertRecord(rec, outRid);
    if (status == OK) {
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true; // Page is dirty
        return OK;
    } else if (status == NOSPACE) {
        // Current page is full; allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
            return status;

        // Initialize the new page
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1); // No next page
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Link up new page appropriately
        status = curPage->setNextPage(newPageNo); // Set forward pointer
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Update header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // Unpin the old current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) {
            bufMgr->unPinPage(filePtr, newPageNo, true);
            return status;
        }

        // Make the new page the current page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        // Insert the record into the new page
        status = curPage->insertRecord(rec, outRid);
        if (status == OK) {
            headerPage->recCnt++;
            return OK;
        } else {
            return status;
        }
    } else {
        return status;
    }
}
