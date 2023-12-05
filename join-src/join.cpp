#include <cassert>
#include <iostream>
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
        address_table_sizes[i] = 0;
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

bool eject_slot(bucket_t buckets[NUM_BUCKETS], hash_t hash, slotidx_t slot_idx)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[slot_idx];

    assert(slot.status != 0); // slot must be occupied

    bucket_t &bucket_alt = buckets[slot.tag];
    slot_t &slot_alt = bucket_alt.slots[slot_idx];

    // slot is free
    if (slot_alt.status == 0)
    {
        // slot being ejected CANNOT be chained
        /** AFSOC this slot is chained. The only way a slot can be chained
         * is if we tried to eject it and failed, and then the new tuple
         * was added to the slot's chain. But we have
         * have just successfully ejected this slot. This is a
         * contradiction[]
         */
        slot_alt.status = 1;
        slot_alt.tag = hash;
        slot_alt.head = slot.head;

        return true;
    }

    return false;
}

void start_at_empty_chain(atindex_t address_table_sizes[NUM_SLOTS],
                          slotidx_t slot_idx,
                          hash_t tag,
                          slot_t *slot,
                          slotidx_t *slot_out,
                          atindex_t *insert_loc,
                          atindex_t *head)
{
    atindex_t addr_table_size = address_table_sizes[slot_idx];
    *insert_loc = addr_table_size;
    address_table_sizes[slot_idx]++;
    *head = addr_table_size;
    *slot_out = slot_idx;

    slot->status = 1;
    slot->tag = tag;
    slot->head = addr_table_size;
    return;
}

void select_slot(bucket_t buckets[NUM_BUCKETS],
                 atindex_t address_table_sizes[NUM_SLOTS],
                 hash_t hash1_val,
                 hash_t hash2_val,
                 hash_t *hash_out,
                 slotidx_t *slot_out,
                 atindex_t *insert_loc,
                 atindex_t *head)
{
    // try to eject slots
    for (slotidx_t slot_idx = 0; slot_idx < NUM_SLOTS; slot_idx++)
    {
        slot_t &slot1 = buckets[hash1_val].slots[slot_idx];
        slot_t &slot2 = buckets[hash2_val].slots[slot_idx];
        if (eject_slot(buckets, hash1_val, slot_idx))
        {
            std::cout << "ejected slot " << slot_idx << std::endl;
            *hash_out = hash1_val;

            start_at_empty_chain(address_table_sizes, slot_idx, hash2_val, &slot1, slot_out, insert_loc, head);
            return;
        }
        if (eject_slot(buckets, hash2_val, slot_idx))
        {
            std::cout << "ejected slot " << slot_idx << std::endl;
            *hash_out = hash2_val;

            start_at_empty_chain(address_table_sizes, slot_idx, hash1_val, &slot2, slot_out, insert_loc, head);
            return;
        }
    }

    // default to collision slot
    slotidx_t slot_idx = buckets[hash1_val].collision_slot;
    buckets[hash1_val].collision_slot = (buckets[hash1_val].collision_slot + 1) % NUM_SLOTS;

    *hash_out = hash1_val;
    *slot_out = slot_idx;

    // set insert location to end of address table
    *insert_loc = address_table_sizes[slot_idx];
    address_table_sizes[slot_idx]++;
    // set head to slot head since we are adding to the collision chain
    *head = buckets[hash1_val].slots[slot_idx].head;
    return;
}

void insert_tuple_to_address_table(address_table_t *address_table, atindex_t head, atindex_t insert_loc, RID_t rid, Key_t key)
{
    // insert as tail of collision chain
    /** (why tho????) so that the head of the chain stays the same. That
     * way, the only data structure accessed here is the address table,
     * and the head pointer in the slot does not need to be updated.
     */
    atindex_t curr = head;
    while (address_table->entries[curr].next != 0)
    {
        curr = address_table->entries[curr].next;
    }

    address_table->entries[curr].next = insert_loc;

    // insert new tuple
    address_table->entries[insert_loc].rid = rid;
    address_table->entries[insert_loc].key = key;
    address_table->entries[insert_loc].next = 0;

    /**
     * Observe that if head == insert_loc, then we create 1 length chain.
     */

    return;
}

bool find_empty_slot(bucket_t buckets[NUM_BUCKETS],
                     atindex_t address_table_sizes[NUM_SLOTS],
                     hash_t hash1_val,
                     hash_t hash2_val,
                     hash_t *hash_out,
                     slotidx_t *slot_out,
                     atindex_t *insert_loc,
                     atindex_t *head)
{
        slotidx_t slot1_idx = buckets[hash1_val].collision_slot;
        slotidx_t slot2_idx = buckets[hash2_val].collision_slot;
        slot_t &slot1 = buckets[hash1_val].slots[slot1_idx];
        slot_t &slot2 = buckets[hash2_val].slots[slot2_idx];
        if (slot1.status == 0)
        {
            buckets[hash1_val].collision_slot = (buckets[hash1_val].collision_slot + 1) % NUM_SLOTS;

            *hash_out = hash1_val;
            start_at_empty_chain(address_table_sizes, slot1_idx, hash2_val, &slot1, slot_out, insert_loc, head);
            return true;
        }
        if (slot2.status == 0)
        {
            buckets[hash2_val].collision_slot = (buckets[hash2_val].collision_slot + 1) % NUM_SLOTS;

            *hash_out = hash2_val;
            start_at_empty_chain(address_table_sizes, slot2_idx, hash1_val, &slot2, slot_out, insert_loc, head);
            return true;
        }
    return false;
}

void CHashJoin::Build(std::vector<input_tuple_t> &relR)
{
    for (input_tuple_t &tuple : relR)
    {
        // first try to insert into an empty slot
        hash_t hash1_val = hash1(tuple.key);
        hash_t hash2_val = hash2(tuple.key);
        bool slot_found = false;
        hash_t insert_bucket;
        slotidx_t insert_slot;
        atindex_t insert_loc;
        atindex_t head;

        slot_found = find_empty_slot(buckets, address_table_sizes, hash1_val, hash2_val, &insert_bucket, &insert_slot, &insert_loc, &head);

        if (!slot_found)
        {
            std::cout << "slot not found for tuple with key " << tuple.key << std::endl;
            select_slot(buckets, address_table_sizes, hash1_val, hash2_val, &insert_bucket, &insert_slot, &insert_loc, &head);
        }
        insert_tuple_to_address_table(&address_tables[insert_slot], head, insert_loc, tuple.rid, tuple.key);
    }
}

bool CHashJoin::search_slot(hash_t hash, slotidx_t slot_idx, RID_t rid, Key_t key, std::vector<output_tuple_t> &relRS)
{
    bucket_t &bucket = buckets[hash];
    slot_t &slot = bucket.slots[slot_idx];

    if (slot.status == 0)
    {
        return false;
    }

    address_table_t &address_table = address_tables[slot_idx];
    atindex_t curr = slot.head;

    // search collision chain
    do
    {
        if (address_table.entries[curr].key == key)
        {
            relRS.push_back({address_table.entries[curr].rid, rid, key});
            // only need to find one match
            return true;
        }
        curr = address_table.entries[curr].next;
    } while (curr != 0);

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
            if (search_slot(hash1_val, slot_idx, tuple.rid, tuple.key, relRS))
            {
                break;
            }
            if (search_slot(hash2_val, slot_idx, tuple.rid, tuple.key, relRS))
            {
                break;
            }
        }
    }
}