#include <cassert>
#include "join.h"

CHashJoin::CHashJoin()
{
    for (int i = 0; i < NUM_BUCKETS; i++)
    {
        for (int j = 0; j < NUM_SLOTS; j++)
        {
            buckets[i].slots[j].status = 0;
            buckets[i].slots[j].tag = 0;
            buckets[i].slots[j].head = 0;
        }
        buckets[i].collision_slot = i % NUM_SLOTS;
    }

    for (int i = 0; i < NUM_SLOTS; i++)
    {
        for (int j = 0; j < NUM_ADDRESS_TABLES_SLOT; j++)
        {
            address_tables[i].entries[j].rid = 0;
            address_tables[i].entries[j].key = 0;
            address_tables[i].entries[j].next = 0;
        }
        address_tables[i].size = 0;
    }
}

hash_t hash1(Key_t key)
{
    unsigned int x = static_cast<unsigned int>(key);
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
    return y;
}

hash_t hash2(Key_t key)
{
    unsigned int x = static_cast<unsigned int>(key);
    hash_t y = static_cast<hash_t>(x & ((1 << HASH_BITS) - 1));
    return y;
}

void CHashJoin::Join(std::vector<input_tuple_t> &relR, std::vector<input_tuple_t> &relS, std::vector<output_tuple_t> &relRS)
{
    Build(relR);
    Probe(relS, relRS);
}

bool CHashJoin::insert_tuple_to_empty_slot(hash_t hash, atindex_t slot_idx, RID_t rid, Key_t key, hash_t tag)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[slot_idx];

    if (slot.status != 0)
    {
        return false;
    }

    address_table_t &address_table = address_tables[slot_idx];
    atindex_t next = address_table.size;

    slot.status = 1;
    slot.tag = tag;
    slot.head = next;

    address_table.entries[next].rid = rid;
    address_table.entries[next].key = key;
    address_table.entries[next].next = 0;
    address_table.size++;

    return true;
}

bool CHashJoin::eject_slot(hash_t hash, slotidx_t slot_idx)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[slot_idx];

    assert(slot.status != 0); // slot must be occupied

    bucket_t &bucket_alt = buckets[slot.tag];
    for (int i = 0; i < NUM_SLOTS; i++)
    {
        slot_t &slot_alt = bucket_alt.slots[i];
        if (slot_alt.status == 0)
        {
            slot_alt.status = 1;
            slot_alt.tag = slot.tag;
            slot_alt.head = slot.head;
            slot.status = 0;
            slot.tag = 0;
            slot.head = 0;
            return true;
        }
    }

    return false;
}

void CHashJoin::insert_tuple_to_collision_slot(hash_t hash, RID_t rid, Key_t key)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[bucket.collision_slot];

    assert(slot.status != 0); // slot must be occupied

    address_table_t &address_table = address_tables[bucket.collision_slot];
    atindex_t next = address_table.size;

    address_table.entries[next].rid = rid;
    address_table.entries[next].key = key;
    address_table.entries[next].next = slot.head;
    address_table.size++;

    // insert as tail of collision chain (why tho????)
    atindex_t curr = slot.head;
    while (address_table.entries[curr].next != 0)
    {
        curr = address_table.entries[curr].next;
    }

    address_table.entries[curr].next = next;

    bucket.collision_slot = (bucket.collision_slot + 1) % NUM_SLOTS;
    return;
}

void CHashJoin::Build(std::vector<input_tuple_t> &relR)
{
    for (input_tuple_t &tuple : relR)
    {
        // first try to insert into an empty slot
        hash_t hash1_val = hash1(tuple.key);
        hash_t hash2_val = hash2(tuple.key);
        bool inserted = false;
        for (int slot_idx = 0; slot_idx < NUM_SLOTS; slot_idx++)
        {
            if (insert_tuple_to_empty_slot(hash1_val, slot_idx, tuple.rid, tuple.key, hash2_val))
            {
                inserted = true;
                break;
            }
            if (insert_tuple_to_empty_slot(hash2_val, slot_idx, tuple.rid, tuple.key, hash1_val))
            {
                inserted = true;
                break;
            }
        }

        if (inserted)
            continue;

        // next try to eject a slot
        for (int slot_idx = 0; slot_idx < NUM_SLOTS; slot_idx++)
        {
            if (eject_slot(hash1_val, slot_idx))
            {
                inserted = insert_tuple_to_empty_slot(hash1_val, slot_idx, tuple.rid, tuple.key, hash2_val);
                assert(inserted);
                break;
            }
            if (eject_slot(hash2_val, slot_idx))
            {
                inserted = insert_tuple_to_empty_slot(hash2_val, slot_idx, tuple.rid, tuple.key, hash1_val);
                assert(inserted);
                break;
            }
        }

        if (inserted)
            continue;

        // next add to chain at hash1 bucket collision slot
        insert_tuple_to_collision_slot(hash1_val, tuple.rid, tuple.key);
    }
}

bool CHashJoin::search_slot(hash_t hash, slotidx_t slot_idx, Key_t key, std::vector<output_tuple_t> &relRS)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[slot_idx];

    if (slot.status == 0)
    {
        return;
    }

    address_table_t &address_table = address_tables[slot_idx];
    atindex_t curr = slot.head;

    while (curr != 0)
    {
        if (address_table.entries[curr].key == key)
        {
            relRS.push_back({address_table.entries[curr].rid, 0, address_table.entries[curr].key});
            // only need to find one match
            return true;
        }
        curr = address_table.entries[curr].next;
    }

    return false;
}

void CHashJoin::Probe(std::vector<input_tuple_t> &relS, std::vector<output_tuple_t> &relRS)
{
    for (input_tuple_t &tuple : relS)
    {
        hash_t hash1_val = hash1(tuple.key);
        hash_t hash2_val = hash2(tuple.key);

        for (int slot_idx = 0; slot_idx < NUM_SLOTS; slot_idx++)
        {
            if (search_slot(hash1_val, slot_idx, tuple.key, relRS))
            {
                break;
            }
            if (search_slot(hash2_val, slot_idx, tuple.key, relRS))
            {
                break;
            }
        }
    }
}