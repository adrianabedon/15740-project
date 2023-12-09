#include "kml_join.hpp"
#include <iostream>

typedef struct tuple_stream_in
{
  RID_t rid;
  Key_t key;
  hash_t hash1;
  hash_t hash2;
} tuple_stream_in_t;

typedef struct select_stream_in
{
  tuple_stream_in_t tuple;
  bool inserted;
  slotidx_t insert_slot;
  atindex_t at_insert_loc;
  atindex_t head;
} select_stream_in_t;

typedef struct insert_steam
{
  RID_t rid;
  Key_t key;
  slotidx_t insert_slot;
  atindex_t at_insert_loc;
  atindex_t head;
} insert_stream_t;

static hash_t hash1(Key_t key)
{
#pragma HLS INLINE
  unsigned int x = static_cast<unsigned int>(key);
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = (x >> 16) ^ x;
  hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
  return y;
}

static hash_t hash2(Key_t key)
{
#pragma HLS INLINE
  unsigned int x = static_cast<unsigned int>(key);
  hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
  return y;
}

static void get_new_tuple(input_tuple_t *relR, hls::stream<tuple_stream_in_t> &tuple_stream, int numR)
{
  for (int i = 0; i < numR; i++)
  {
    tuple_stream_in_t tuple;
    tuple.rid = relR[i].rid;
    tuple.key = relR[i].key;
    tuple.hash1 = hash1(tuple.key);
    tuple.hash2 = hash2(tuple.key);
    tuple_stream.write(tuple);
  }
}

static void find_empty_slot(bucket_t buckets[NUM_BUCKETS],
                            atindex_t address_table_sizes[NUM_SLOTS],
                            hls::stream<tuple_stream_in_t> &tuple_stream,
                            hls::stream<select_stream_in_t> &select_stream,
                            int numR)
{
  for (int i = 0; i < numR; i++)
  {
    tuple_stream_in_t tuple = tuple_stream.read();
    hash_t hash1 = tuple.hash1;
    hash_t hash2 = tuple.hash2;
    slotidx_t slot1_idx = buckets[hash1].collision_slot;
    slotidx_t slot2_idx = buckets[hash2].collision_slot;
    slotidx_t insert_slot;
    hash_t insert_bucket;
    hash_t tag;
    if (buckets[hash1].slots[slot1_idx].status == 0)
    {
      insert_slot = slot1_idx;
      insert_bucket = hash1;
      tag = hash2;
    }
    else if (buckets[hash2].slots[slot2_idx].status == 0)
    {
      insert_slot = slot2_idx;
      insert_bucket = hash2;
      tag = hash1;
    }
    else
    {
      select_stream_in_t select_stream_out;
      select_stream_out.tuple = tuple;
      select_stream_out.inserted = false;
      select_stream.write(select_stream_out);
      continue;
    }
    /** Insert into the selected slot */
    // TODO see if you need % NUM_SLOTS
    buckets[insert_bucket].collision_slot = (buckets[insert_bucket].collision_slot + 1);

    atindex_t addr_table_size = address_table_sizes[insert_slot];
    address_table_sizes[insert_slot] = addr_table_size + 1;

    buckets[insert_bucket].slots[insert_slot].status = 1;
    buckets[insert_bucket].slots[insert_slot].tag = tag;
    buckets[insert_bucket].slots[insert_slot].head = addr_table_size;

    select_stream_in_t select_stream_out;
    select_stream_out.tuple = tuple;
    select_stream_out.inserted = true;
    select_stream_out.insert_slot = insert_slot;
    select_stream_out.at_insert_loc = addr_table_size;
    select_stream_out.head = addr_table_size;
    select_stream.write(select_stream_out);
  }
}

bool eject_slot(bucket_t buckets[NUM_BUCKETS], hash_t hash, slotidx_t slot_idx)
{
  hash_t bucket_alt_index = buckets[hash].slots[slot_idx].tag;

  // slot is free
  if (buckets[bucket_alt_index].slots[slot_idx].status == 0)
  {
    atindex_t head = buckets[hash].slots[slot_idx].head;
    // slot being ejected CANNOT be chained
    /** AFSOC this slot is chained. The only way a slot can be chained
     * is if we tried to eject it and failed, and then the new tuple
     * was added to the slot's chain. But we have
     * have just successfully ejected this slot. This is a
     * contradiction []
     */
    buckets[bucket_alt_index].slots[slot_idx].status = 1;
    buckets[bucket_alt_index].slots[slot_idx].tag = hash;
    buckets[bucket_alt_index].slots[slot_idx].head = head;

    return true;
  }

  return false;
}

static void select_slot(bucket_t buckets[NUM_BUCKETS],
                        atindex_t address_table_sizes[NUM_SLOTS],
                        hls::stream<select_stream_in_t> &select_stream,
                        hls::stream<insert_stream_t> &insert_stream,
                        int numR)
{
  for (int i = 0; i < numR; i++)
  {
    select_stream_in_t select_in = select_stream.read();
    if (select_in.inserted)
    {
      // already inserted into an empty slot, now insert into address table
      insert_stream_t insert_stream_out;
      insert_stream_out.rid = select_in.tuple.rid;
      insert_stream_out.key = select_in.tuple.key;
      insert_stream_out.insert_slot = select_in.insert_slot;
      insert_stream_out.at_insert_loc = select_in.at_insert_loc;
      insert_stream_out.head = select_in.head;
      insert_stream.write(insert_stream_out);
      continue;
    }

    // Not inserted into an empty slot, try to eject a slot

    tuple_stream_in_t tuple = select_in.tuple;
    hash_t hash1 = tuple.hash1;
    hash_t hash2 = tuple.hash2;
    bool ejected = false;
    slotidx_t insert_slot;
    hash_t insert_bucket;
    hash_t tag;
    atindex_t at_insert_loc;
    atindex_t head;

    /** Try to eject a slot. */
    for (int slot_idx_ = 0; slot_idx_ < NUM_SLOTS; slot_idx_++)
    {
      slotidx_t slot_idx = (slotidx_t)(unsigned int)slot_idx_;
#pragma HLS unroll
      if (eject_slot(buckets, hash1, slot_idx))
      {
        ejected = true;
        insert_slot = slot_idx;
        insert_bucket = hash1;
        tag = hash2;
      }
      if (eject_slot(buckets, hash2, slot_idx))
      {
        ejected = true;
        insert_slot = slot_idx;
        insert_bucket = hash2;
        tag = hash1;
      }
    }

    if (ejected)
    {
      // if ejected, then insert into the ejected slot
      atindex_t addr_table_size = address_table_sizes[insert_slot];
      address_table_sizes[insert_slot] = addr_table_size + 1;

      buckets[insert_bucket].slots[insert_slot].status = 1;
      buckets[insert_bucket].slots[insert_slot].tag = tag;
      buckets[insert_bucket].slots[insert_slot].head = addr_table_size;

      head = addr_table_size;
      at_insert_loc = addr_table_size;
    }
    else
    {
      // if not eject, then insert into the chain of collision slot
      // no need to change the slot's status, tag, or head
      insert_slot = buckets[hash1].collision_slot;
      // TODO see if you need % NUM_SLOTS
      buckets[hash1].collision_slot = (insert_slot + 1);

      atindex_t addr_table_size = address_table_sizes[insert_slot];
      address_table_sizes[insert_slot] = addr_table_size + 1;

      head = buckets[hash1].slots[insert_slot].head;
      at_insert_loc = addr_table_size;
    }

    insert_stream_t insert_stream_out;
    insert_stream_out.rid = tuple.rid;
    insert_stream_out.key = tuple.key;
    insert_stream_out.insert_slot = insert_slot;
    insert_stream_out.at_insert_loc = at_insert_loc;
    insert_stream_out.head = head;
    insert_stream.write(insert_stream_out);
  }
}

static void insert_into_address_table(address_table_t address_tables[NUM_SLOTS],
                                      hls::stream<insert_stream_t> &insert_stream,
                                      int numR)
{
  for (int i = 0; i < numR; i++)
  {
    insert_stream_t insert_stream_in = insert_stream.read();

    // insert new tuple
    address_tables[insert_stream_in.insert_slot].entries[insert_stream_in.at_insert_loc].rid = insert_stream_in.rid;
    address_tables[insert_stream_in.insert_slot].entries[insert_stream_in.at_insert_loc].key = insert_stream_in.key;
    address_tables[insert_stream_in.insert_slot].entries[insert_stream_in.at_insert_loc].next = 0;

    if (insert_stream_in.head == insert_stream_in.at_insert_loc)
    {
      // start empty chain
      continue;
    }

    // Insert into chain
    atindex_t curr = insert_stream_in.head;
    while (address_tables[insert_stream_in.insert_slot].entries[curr].next != 0)
    {
      curr = address_tables[insert_stream_in.insert_slot].entries[curr].next;
    }

    address_tables[insert_stream_in.insert_slot].entries[curr].next = insert_stream_in.at_insert_loc;

    /**
     * Observe that if head == at_insert_loc, then we create 1 length chain.
     */
  }
}

static void build(bucket_t buckets[NUM_BUCKETS], address_table_t address_tables[NUM_SLOTS], input_tuple_t *relR, int numR)
{
  atindex_t address_table_sizes[NUM_SLOTS];
#pragma HLS ARRAY_RESHAPE variable = address_tables type = complete dim = 1

  for (int i = 0; i < NUM_SLOTS; i++)
  {
#pragma HLS unroll
    address_table_sizes[i] = 0;
  }

  static hls::stream<tuple_stream_in_t> tuple_stream;
  static hls::stream<select_stream_in_t> select_stream;
  static hls::stream<insert_stream_t> insert_stream;

#pragma HLS DATAFLOW
  get_new_tuple(relR, tuple_stream, numR);
  find_empty_slot(buckets, address_table_sizes, tuple_stream, select_stream, numR);
  select_slot(buckets, address_table_sizes, select_stream, insert_stream, numR);
  insert_into_address_table(address_tables, insert_stream, numR);
}

static void search_address_table(address_table_t address_tables[NUM_SLOTS],
                                 slotidx_t slot_idx,
                                 RID_t rid,
                                 Key_t key,
                                 atindex_t head,
                                 hls::stream<output_tuple_t> &output_stream,
                                 hls::stream<bool> &eos)
{
  // address_table_entry_t entries[NUM_ADDRESS_TABLES_SLOT] = address_tables[slot_idx].entries;
  atindex_t curr = head;
  int i = 0;
  do
  {
    if (address_tables[slot_idx].entries[curr].key == key)
    {
      output_tuple_t output_tuple;
      output_tuple.rid1 = address_tables[slot_idx].entries[curr].rid;
      output_tuple.rid2 = rid;
      output_tuple.key = key;
      output_stream.write(output_tuple);
      eos.write(false);
      return;
    }
    curr = address_tables[slot_idx].entries[curr].next;
    i++;
  } while (curr != 0);
}

static void search(bucket_t buckets[NUM_BUCKETS],
                   address_table_t address_tables[NUM_SLOTS],
                   hls::stream<tuple_stream_in_t> &tuple_stream,
                   hls::stream<output_tuple_t> &output_stream,
                   hls::stream<bool> &eos,
                   int numS)
{
  for (int i = 0; i < numS; i++)
  {
    tuple_stream_in_t tuple = tuple_stream.read();
    hash_t hash1 = tuple.hash1;
    hash_t hash2 = tuple.hash2;
    
    for (int slot_idx_ = 0; slot_idx_ < NUM_SLOTS; slot_idx_++)
    {
#pragma HLS unroll
      slotidx_t slot_idx = (slotidx_t)(unsigned int)slot_idx_;
      if (buckets[hash1].slots[slot_idx].status == 1)
      {
        atindex_t head = buckets[hash1].slots[slot_idx].head;
        search_address_table(address_tables, slot_idx, tuple.rid, tuple.key, head, output_stream, eos);
      }
      if (buckets[hash2].slots[slot_idx].status == 1)
      {
        atindex_t head = buckets[hash2].slots[slot_idx].head;
        search_address_table(address_tables, slot_idx, tuple.rid, tuple.key, head, output_stream, eos);
      }
    }
  }
  eos.write(true);
}

static void write_output(hls::stream<output_tuple_t> &output_stream,
                         hls::stream<bool> &eos,
                         output_tuple_t *relRS)
{
  int i = 0;
  while (1)
  {
    bool end = eos.read();
    if (end)
    {
      break;
    }
    output_tuple_t output_tuple = output_stream.read();
    relRS[i] = output_tuple;
    i++;
  }
}

static void probe(bucket_t buckets[NUM_BUCKETS], address_table_t address_tables[NUM_SLOTS], input_tuple_t *relS, output_tuple_t *relRS, int numS)
{
  static hls::stream<tuple_stream_in_t> tuple_stream;
  static hls::stream<output_tuple_t> output_stream;
  hls::stream<bool> eos;

#pragma HLS DATAFLOW
  get_new_tuple(relS, tuple_stream, numS);
  search(buckets, address_tables, tuple_stream, output_stream, eos, numS);
  write_output(output_stream, eos, relRS);
}

extern "C"
{
  void kml_join(input_tuple_t *relR,
                input_tuple_t *relS,
                output_tuple_t *relRS,
                int numR,
                int numS)
  {
    bucket_t buckets[NUM_BUCKETS];
    address_table_t address_tables[NUM_SLOTS];
#pragma HLS ARRAY_RESHAPE variable = address_tables type = complete dim = 1

    for (int i = 0; i < NUM_BUCKETS; i++)
    {
      buckets[i].collision_slot = i & (NUM_SLOTS - 1);
      for (int j = 0; j < NUM_SLOTS; j++)
      {
#pragma HLS unroll
        buckets[i].slots[j].status = 0;
      }
    }

    build(buckets, address_tables, relR, numR);
    probe(buckets, address_tables, relS, relRS, numS);
  }
}