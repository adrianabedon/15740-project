#include "kml_join.hpp"

typedef struct tuple_stream_in
{
  RID_t rid;
  Key_t key;
  Hash_t hash1;
  Hash_t hash2;
} tuple_stream_in_t;

typedef struct insert_steam
{
  RID_t rid;
  Key_t key;
  slotidx_t insert_slot;
  atindex_t at_insert_loc;
  atindex_t head;
} insert_stream_t;

hash_t hash1(Key_t key)
{
#pragma HLS INLINE
  unsigned int x = static_cast<unsigned int>(key);
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = ((x >> 16) ^ x) * 0x45d9f3b;
  x = (x >> 16) ^ x;
  hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
  return y;
}

hash_t hash2(Key_t key)
{
#pragma HLS INLINE
  unsigned int x = static_cast<unsigned int>(key);
  hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
  return y;
}

void get_new_tuple(input_tuple_t *relR, hls::stream<tuple_stream_in_t> &tuple_stream, int numR)
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

void find_empty_slot(bucket_t buckets[NUM_BUCKETS],
                     atindex_t address_table_sizes[NUM_SLOTS],
                     hls::stream<tuple_stream_in_t> &tuple_stream,
                     hls::stream<insert_stream_t> &insert_stream,
                     hls::stream<tuple_stream_in_t> &select_stream)
{
  tuple_stream_in_t tuple;
  
}

static void build(input_tuple_t *relR, int numR, bucket_t buckets[NUM_BUCKETS], address_table_t address_tables[NUM_SLOTS])
{
  atindex_t address_table_sizes[NUM_SLOTS];
#pragma HLS ARRAY_RESHAPE variable = address_tables type = complete dim = 1

  static hls::stream<tuple_stream_in_t> tuple_stream;
  static hls::stream<insert_stream_t> insert_stream;
  static hls::stream<tuple_stream_in_t> select_stream;

#pragma HLS DATAFLOW
  get_new_tuple(relR, tuple_stream, numR);
  find_empty_slot(buckets, address_table_sizes, tuple_stream, insert_stream, select_stream);

}

extern "C"
{
  void krnl_join(input_tuple_t *relR,
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
        buckets[i].slots[j].status = 0;
      }
    }

    build(relR, numR, buckets, address_tables);
  }
}