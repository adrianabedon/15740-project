#include "xcl2.hpp"

#define HASH_BITS 12
#define NUM_SLOTS 4
#define NUM_BUCKETS (2 << HASH_BITS) // 2^12 = 4096
#define NUM_ADDRESS_TABLES_SLOT 5000 // this is the number of slots in each address table
                                     // max size of inner relation in NUM_SLOT * NUM_ADDRESS_TABLES_SLOT

// they use 12 bit wide hash function
/** We are implementing N to 1 join, so if the tuple in the second
 * relation can only map to one tuple in the first relation.
 */

typedef ap_uint<2> slotidx_t;
typedef ap_uint<HASH_BITS> hash_t;
typedef uint16_t atindex_t;
typedef int RID_t;
typedef int Key_t;
typedef struct
{
  RID_t rid;
  Key_t key;
} input_tuple_t;
typedef struct
{
  RID_t rid1;
  RID_t rid2;
  Key_t key;
} output_tuple_t;

typedef struct
{
  // occupied or availale (only need 1 bit)
  char status;
  // candidate bucket number of this element
  // Value of second hash function (12 bits)
  hash_t tag;
  // entry point of first tuple in this slot (in the address tables) (log_2_N bits (N is size of first relation))
  atindex_t head;
} slot_t;

typedef struct
{
  slot_t slots[NUM_SLOTS];
  // This is the slot to insert into for a collision chain
  // only need 2 bits
  // 1. Initiate once in round robin fashion
  // 2. Increment its value each time it is used (mod 4 but don't need to worry on FPGA)
  slotidx_t collision_slot;
} bucket_t;

typedef struct
{
  RID_t rid;
  Key_t key;      // K bits (K = 32 here)
  atindex_t next; // next tuple
} address_table_entry_t;

typedef struct
{
  address_table_entry_t entries[NUM_ADDRESS_TABLES_SLOT];
} address_table_t;