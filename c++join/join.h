#ifndef __JOIN_H__
#define __JOIN_H__

#include <vector>
#include <cstdint>

#define HASH_BITS 12
#define NUM_SLOTS 4
#define NUM_BUCKETS (2 << HASH_BITS) // 2^12 = 4096
#define NUM_ADDRESS_TABLES_SLOT 10000 // this is the number of slots in each address table
                                     // max size of inner relation in NUM_SLOT * NUM_ADDRESS_TABLES_SLOT

// they use 12 bit wide hash function
/** We are implementing N to 1 join, so if the tuple in the second
 * relation can only map to one tuple in the first relation.
 */

typedef uint8_t slotidx_t;
typedef uint16_t hash_t;
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

class CHashJoin
{
public:
  CHashJoin();
  ~CHashJoin();
  /**
   * @brief
   *
   * @param relR Inner relation
   * @param relS Outer relation
   * @param relRS Result relation #TODO FIX THIS TYPE
   *
   */
  void Join(std::vector<input_tuple_t> &relR, std::vector<input_tuple_t> &relS, std::vector<output_tuple_t> &relRS);

private:
  // bool find_empty_slot(hash_t hash1_val, hash_t hash2_val, hash_t *hash_out, slotidx_t *slot_out);
  // bool insert_tuple_to_empty_slot(hash_t hash, atindex_t slot_idx, RID_t rid, Key_t key, hash_t tag);
  // void insert_tuple_to_collision_slot(hash_t hash, RID_t rid, Key_t key);
  // bool eject_slot(hash_t hash, slotidx_t slot_idx);
  bool search_slot(hash_t hash, slotidx_t slot_idx, RID_t rid, Key_t key, std::vector<output_tuple_t> &relRS);
  void Build(std::vector<input_tuple_t> &relR);
  void Probe(std::vector<input_tuple_t> &relS, std::vector<output_tuple_t> &relRS);

  bucket_t buckets[NUM_BUCKETS];
  atindex_t address_table_sizes[NUM_SLOTS];
  // NUM_SLOTS address tables
  address_table_t address_tables[NUM_SLOTS];
};

#endif // __JOIN_H__
