from template.bufferpool import Bufferpool

START_RID = 0
PAGE_SETS = 16
PAGE_SIZE = 4096
RECORDS_PER_PAGE = PAGE_SIZE // 8
SEED_REBUILD_THRESH = 1.15  # Rebuild seeds after size has changed by last_size * SEED_REBUILD_THRESH
META_DATA_PAGES = 5
PAGES_PER_KEY_DIRECTORY_SET = 5
KEY_DIRECTORY_SET_SIZE = PAGES_PER_KEY_DIRECTORY_SET * PAGE_SIZE
BASE_RID_TYPE = 0
TAIL_RID_TYPE = 1
BASE_RID_TYPE_COPY = 4
TAIL_RID_TYPE_COPY = 5

DELETED_WT_RID_TYPE = 2
DELETED_NT_RID_TYPE = 3
NO_RID_TYPE = 4
BUFFER_POOL = Bufferpool()
MERGE_TIMER_INTERVAL = 7 # every 7 seconds, the merge check will be called
MERGE_THRESHOLD = 100 # how many base records out of a base page set must be updated to perform a merge
NUMBER_OF_BASE_PAGE_SETS_TO_CHECK = 3 # at any given time, we will check 3 base page sets to merge


def init():
    pass
