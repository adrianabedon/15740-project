#include "kml_join.hpp"
#include <iostream>

const unsigned int num_tuples = NUM_TUPLES;

typedef struct tuple_stream_in
{
  RID_t rid;
  Key_t key;
  hash_t hash1;
  hash_t hash2;
} tuple_stream_in_t;

typedef struct insert_steam
{
  RID_t rid;
  Key_t key;
  atindex_t at_insert_loc;
  atindex_t head;
} insert_stream_t;

typedef struct
{
  RID_t rid;
  Key_t key;
  status_t statuses[8];
  atindex_t curr[8];
} lookup_stream_t;

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

bool eject_slot(bucket_t buckets[NUM_BUCKETS], hash_t hash, slotidx_t slot_idx)
{
#pragma HLS INLINE
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

static void find_slot(bucket_t buckets[NUM_BUCKETS],
                      hls::stream<tuple_stream_in_t> &tuple_stream,
                      hls::stream<insert_stream_t> insert_streams[NUM_SLOTS],
                      hls::stream<bool> eos[NUM_SLOTS],
                      int numR)
{
  atindex_t address_table_sizes[NUM_SLOTS];
#pragma HLS ARRAY_RESHAPE variable = address_table_sizes type = complete dim = 1

  /** Initialize structures */
  for (int i = 0; i < NUM_BUCKETS; i++)
  {
    buckets[i].collision_slot = i & (NUM_SLOTS - 1);
    for (int j = 0; j < NUM_SLOTS; j++)
    {
#pragma HLS unroll
      buckets[i].slots[j].status = 0;
    }
  }

  for (int i = 0; i < NUM_SLOTS; i++)
  {
#pragma HLS unroll
    address_table_sizes[i] = 0;
  }

  /** Start finding slots */

  for (int i = 0; i < numR; i++)
  {
#pragma HLS PIPELINE off
    tuple_stream_in_t tuple = tuple_stream.read();
    hash_t hash1 = tuple.hash1;
    hash_t hash2 = tuple.hash2;
    slotidx_t insert_slot;
    hash_t insert_bucket;
    hash_t tag;
    atindex_t head;
    bool chain = true;

    for (int slot_idx_ = 0; slot_idx_ < NUM_SLOTS; slot_idx_++)
    {
#pragma HLS unroll
      if (buckets[hash1].slots[slot_idx_].status == 0)
      {
        chain = false;
        insert_slot = slot_idx_;
        insert_bucket = hash1;
        tag = hash2;
      }
      else if (hash1 != hash2 && buckets[hash2].slots[slot_idx_].status == 0)
      {
        chain = false;
        insert_slot = slot_idx_;
        insert_bucket = hash2;
        tag = hash1;
      }
    }

    if (chain)
    {
      /** No empty slot, try to eject a slot. */
      for (int slot_idx_ = 0; slot_idx_ < NUM_SLOTS; slot_idx_++)
      {
#pragma HLS unroll
        slotidx_t slot_idx = (slotidx_t)(unsigned int)slot_idx_;
        if (eject_slot(buckets, hash1, slot_idx))
        {
          chain = false;
          insert_slot = slot_idx;
          insert_bucket = hash1;
          tag = hash2;
          break;
        }
        else if (hash1 != hash2 && eject_slot(buckets, hash2, slot_idx))
        {
          chain = false;
          insert_slot = slot_idx;
          insert_bucket = hash2;
          tag = hash1;
          break;
        }
      }

      if (chain)
      {
        // if not eject, then insert into the chain of collision slot
        // no need to change the slot's status, tag, or head
        insert_slot = buckets[hash1].collision_slot;
        insert_bucket = hash1;
        buckets[hash1].collision_slot = (buckets[hash1].collision_slot + 1);
      }
    }


    /** Insert into the selected slot */
    buckets[insert_bucket].slots[insert_slot].status = 1;

    /** Insert at the end of the address table. */
    atindex_t at_insert_loc = address_table_sizes[insert_slot];
    address_table_sizes[insert_slot] = at_insert_loc + 1;

    if (chain)
    {
      head = buckets[insert_bucket].slots[insert_slot].head;
    }
    else
    {
      head = at_insert_loc;
      buckets[insert_bucket].slots[insert_slot].tag = tag;
      buckets[insert_bucket].slots[insert_slot].head = head;
    }

    insert_stream_t insert_stream_out;
    insert_stream_out.rid = tuple.rid;
    insert_stream_out.key = tuple.key;
    insert_stream_out.at_insert_loc = at_insert_loc;
    insert_stream_out.head = head;
    eos[insert_slot].write(false);
    insert_streams[insert_slot].write(insert_stream_out);
  }

  for (int i = 0; i < NUM_SLOTS; i++)
  {
#pragma HLS unroll
    eos[i].write(true);
  }
}

static void insert_into_address_table(address_table_t *address_table,
                                      hls::stream<insert_stream_t> &insert_stream,
                                      hls::stream<bool> &eos)
{
  // #pragma HLS FUNCTION_INSTANTIATE variable = address_table
  while (1)
  {
#pragma HLS PIPELINE off
    bool end = eos.read();
    if (end)
    {
      break;
    }

    insert_stream_t insert_stream_in = insert_stream.read();

    // insert new tuple
    address_table->entries[insert_stream_in.at_insert_loc].rid = insert_stream_in.rid;
    address_table->entries[insert_stream_in.at_insert_loc].key = insert_stream_in.key;
    address_table->entries[insert_stream_in.at_insert_loc].next = 0;

    if (insert_stream_in.head == insert_stream_in.at_insert_loc)
    {
      // start empty chain
      continue;
    }

    // Insert into chain
    atindex_t curr = insert_stream_in.head;
    while (address_table->entries[curr].next != 0)
    {
      curr = address_table->entries[curr].next;
    }

    address_table->entries[curr].next = insert_stream_in.at_insert_loc;

    /**
     * Observe that if head == at_insert_loc, then we create 1 length chain.
     */
  }
}

static void build(bucket_t buckets[NUM_BUCKETS],
                  address_table_t *address_table1, address_table_t *address_table2,
                  address_table_t *address_table3, address_table_t *address_table4,
                  input_tuple_t *relR, int numR)
{
  static hls::stream<tuple_stream_in_t> tuple_stream;
  static hls::stream<insert_stream_t> insert_streams[NUM_SLOTS];
  static hls::stream<bool> eos[NUM_SLOTS];

/** Need to make tuple_stream a PIPO to prevent deadlocks */
#pragma HLS stream type = pipo variable = tuple_stream
/** As long at the chains don't get absurdley long, then we don't need
 * make this a PIPO since inserts will be faster than searches. This is
 * just my theory at the moment though. */
#pragma HLS stream type = pipo variable = insert_streams

#pragma HLS DATAFLOW
  get_new_tuple(relR, tuple_stream, numR);
  find_slot(buckets, tuple_stream, insert_streams, eos, numR);
  insert_into_address_table(address_table1, insert_streams[0], eos[0]);
  insert_into_address_table(address_table2, insert_streams[1], eos[1]);
  insert_into_address_table(address_table3, insert_streams[2], eos[2]);
  insert_into_address_table(address_table4, insert_streams[3], eos[3]);
}

static void table_lookup(address_table_t *address_table1, address_table_t *address_table2,
                         address_table_t *address_table3, address_table_t *address_table4,
                         hls::stream<lookup_stream_t> &lookup_stream,
                         hls::stream<output_tuple_t> &output_stream,
                         hls::stream<bool> &eos,
                         int numS)
{
  address_table_t *address_tables[4] = {address_table1, address_table2, address_table3, address_table4};
  for (int i = 0; i < numS; i++)
  {
    lookup_stream_t lookup_stream_in = lookup_stream.read();
    bool output = false;
    output_tuple_t output_tuple;
    while (1)
    {
#pragma HLS PIPELINE II = 1
      for (int j = 0; j < 4; j++)
      {
#pragma HLS unroll
        if (lookup_stream_in.statuses[2 * j] == 1)
        {
          if (address_tables[j]->entries[lookup_stream_in.curr[2 * j]].key == lookup_stream_in.key)
          {
            output_tuple.rid1 = address_tables[j]->entries[lookup_stream_in.curr[2 * j]].rid;
            output_tuple.rid2 = lookup_stream_in.rid;
            output_tuple.key = lookup_stream_in.key;
            output = true;
          }
        }
        if (lookup_stream_in.statuses[2 * j + 1] == 1)
        {
          if (address_tables[j]->entries[lookup_stream_in.curr[2 * j + 1]].key == lookup_stream_in.key)
          {
            output_tuple.rid1 = address_tables[j]->entries[lookup_stream_in.curr[2 * j + 1]].rid;
            output_tuple.rid2 = lookup_stream_in.rid;
            output_tuple.key = lookup_stream_in.key;
            output = true;
          }
        }
      }

      if (output)
      {
        output_stream.write(output_tuple);
        eos.write(false);
      }

      // go to next
      for (int j = 0; j < 4; j++)
      {
#pragma HLS unroll
        if (lookup_stream_in.statuses[2 * j] == 1)
        {
          atindex_t next = address_tables[j]->entries[lookup_stream_in.curr[2 * j]].next;
          lookup_stream_in.curr[2 * j] = next;
          if (next == 0)
          {
            lookup_stream_in.statuses[2 * j] = 0;
          }
        }
        if (lookup_stream_in.statuses[2 * j + 1] == 1)
        {
          atindex_t next = address_tables[j]->entries[lookup_stream_in.curr[2 * j + 1]].next;
          lookup_stream_in.curr[2 * j + 1] = next;
          if (next == 0)
          {
            lookup_stream_in.statuses[2 * j + 1] = 0;
          }
        }
      }

      // check if done
      bool done = true;
      for (int j = 0; j < 8; j++)
      {
#pragma HLS unroll
        if (lookup_stream_in.statuses[j] == 1)
        {
          done = false;
          break;
        }
      }

      if (done || output)
      {
        break;
      }
    }
  }
  eos.write(true);
}

static void search(bucket_t buckets[NUM_BUCKETS],
                   hls::stream<tuple_stream_in_t> &tuple_stream,
                   hls::stream<lookup_stream_t> &lookup_stream,
                   //  hls::stream<output_tuple_t> &output_stream,
                   //  hls::stream<bool> &eos,
                   int numS)
{
  for (int i = 0; i < numS; i++)
  {
    tuple_stream_in_t tuple = tuple_stream.read();
    hash_t hash1 = tuple.hash1;
    hash_t hash2 = tuple.hash2;

    lookup_stream_t lookup_stream_out;

#pragma HLS DEPENDENCE variable = buckets->slots->status intra false
#pragma HLS DEPENDENCE variable = buckets->slots->head intra false

    for (int j = 0; j < 4; j++)
    {
#pragma HLS unroll
      lookup_stream_out.statuses[2 * j] = buckets[hash1].slots[j].status;
      lookup_stream_out.statuses[2 * j + 1] = buckets[hash2].slots[j].status;
    }
    for (int j = 0; j < 4; j++)
    {
#pragma HLS unroll
      lookup_stream_out.curr[2 * j] = buckets[hash1].slots[j].head;
      lookup_stream_out.curr[2 * j + 1] = buckets[hash2].slots[j].head;
    }

    lookup_stream_out.rid = tuple.rid;
    lookup_stream_out.key = tuple.key;

    lookup_stream.write(lookup_stream_out);
  }
}

static void write_output(hls::stream<output_tuple_t> &output_stream,
                         hls::stream<bool> &eos,
                         output_tuple_t *relRS,
                         int *numResult)
{
  int i = 0;
  while (1)
  {
    bool end = eos.read();
    if (end)
    {
      *numResult = i;
      break;
    }
    output_tuple_t output_tuple = output_stream.read();
    relRS[i] = output_tuple;
    i++;
  }
}

static void probe(bucket_t buckets[NUM_BUCKETS],
                  address_table_t *address_table1, address_table_t *address_table2,
                  address_table_t *address_table3, address_table_t *address_table4,
                  input_tuple_t *relS,
                  output_tuple_t *relRS,
                  int *numResult,
                  int numS)
{
  static hls::stream<tuple_stream_in_t> tuple_stream;
  static hls::stream<lookup_stream_t> lookup_stream;
  static hls::stream<output_tuple_t> output_stream;
  hls::stream<bool> eos;

/** Need to make tuple_stream a PIPO to prevent deadlocks */
#pragma HLS stream type = pipo variable = tuple_stream
#pragma HLS stream type = pipo variable = lookup_stream
#pragma HLS stream type = pipo variable = output_stream
#pragma HLS stream type = pipo variable = eos

#pragma HLS DATAFLOW
  get_new_tuple(relS, tuple_stream, numS);
  search(buckets, tuple_stream, lookup_stream, numS);
  table_lookup(address_table1, address_table2, address_table3, address_table4, lookup_stream, output_stream, eos, numS);
  write_output(output_stream, eos, relRS, numResult);
}

extern "C"
{
  void kml_join(input_tuple_t *relR,
                input_tuple_t *relS,
                output_tuple_t *relRS,
                int *numResult,
                int numR,
                int numS)
  {
/** You HAVE to set the depth to the size of the input for cosimulation. Otherwise if will
 * fail and the error sucks at explaining what happened. */
#pragma HLS INTERFACE m_axi port = relR depth = num_tuples bundle = gmem0
#pragma HLS INTERFACE m_axi port = relS depth = num_tuples bundle = gmem1
#pragma HLS INTERFACE m_axi port = relRS depth = num_tuples bundle = gmem2
#pragma HLS INTERFACE m_axi port = numResult depth = 1 bundle = gmem2
#pragma HLS INTERFACE s_axilite port = relR bundle = control
#pragma HLS INTERFACE s_axilite port = relS bundle = control
#pragma HLS INTERFACE s_axilite port = relRS bundle = control
#pragma HLS INTERFACE s_axilite port = numResult bundle = control
#pragma HLS INTERFACE s_axilite port = numR bundle = control
#pragma HLS INTERFACE s_axilite port = numS bundle = control
#pragma HLS INTERFACE s_axilite port = return bundle = control
    bucket_t buckets[NUM_BUCKETS];
    address_table_t address_table1;
    address_table_t address_table2;
    address_table_t address_table3;
    address_table_t address_table4;

    build(buckets,
          &address_table1,
          &address_table2,
          &address_table3,
          &address_table4,
          relR,
          numR);
    probe(buckets,
          &address_table1,
          &address_table2,
          &address_table3,
          &address_table4,
          relS,
          relRS,
          numResult,
          numS);
  }
}