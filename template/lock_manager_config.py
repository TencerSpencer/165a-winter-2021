from template.lockManager import LockManager

DISK_ACCESS = 40
NEW_BASE_RID_INSERT = 41
NEW_TAIL_RID_UPDATE = 42
BUFFER_POOL_MEM_ASSIGN = 44
BASE_LOADED = 45
TAIL_LOADED = 46
AVAILABLE_PAGE_RANGE = 47
NEXT_TAIL_BLOCK_CALC = 48
PAGES_MEM_MAP_BLOCK = 49
PIN_PAGE_SET = 50
UNPIN_PAGE_SET = 51
BUFFER_POOL_SPACE = 52
PAGE_SET_EVICTION = 53
PAGE_DIR = 54
X_LOCK_EDIT = 55
S_LOCK_EDIT = 56
KEY_DICT = 57


BRID_BLOCK_START = 58
TRID_BLOCK_START = 59
BRID_TO_TRID = 60
TBLOCK_DIRECTORY = 61
LRU_ENFORCEMENT = 62
PAGE_RANGE_BASE_RID = 63
PAGE_RANGE_TAIL_RID = 64
AVAILABLE_BASE_PAGE_SET = 64
MERGE_MUTEX = 65

LOCK_MANAGER = LockManager()


