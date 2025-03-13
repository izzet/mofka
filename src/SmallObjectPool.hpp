/*
 * (C) 2024 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOFKA_SMALL_OBJECT_POOL_H
#define MOFKA_SMALL_OBJECT_POOL_H

#include <mofka/Exception.hpp>
#include <array>
#include <vector>
#include <cstdint>
#include <memory>
#include <limits>
#include <bitset>
#include <abt.h>

namespace mofka {

template <typename T>
struct SmallObjectPool {

    private:

    struct Chunk;

    struct Entry : public T {
        Chunk* m_owner = nullptr;
        int    m_index = -1;

        Entry() = default;

        template<typename ... Args>
        Entry(Chunk* owner, int index, Args&&... args)
        : T{std::forward<Args>(args)...}
        , m_owner(owner)
        , m_index(index) {}
    };

    struct Chunk {
        std::array<Entry, 64> m_content;
        std::bitset<64>       m_is_used;
    };

    static inline ABT_mutex_memory                    s_mtx_mem = ABT_MUTEX_INITIALIZER;
    static inline std::vector<std::shared_ptr<Chunk>> s_chunks;

    static inline std::shared_ptr<Chunk> GetChunkWithFreeSpace() {
        std::shared_ptr<Chunk> chunk;
        for(size_t i = 0; i < s_chunks.size(); ++i) {
            if(!s_chunks[i]->m_is_used.all()) {
                chunk = s_chunks[i];
                break;
            }
        }
        if(!chunk) {
            chunk = std::make_shared<Chunk>();
            s_chunks.push_back(chunk);
        }
        return chunk;
    }

    static inline int GetFreeSpaceInChunk(Chunk* chunk) {
        const auto& used = chunk->m_is_used;
        for(int i=0; i < 64; ++i) {
            if(!used[i]) return i;
        }
        return -1;
    }

    public:

    template <typename ... Args>
    static inline std::shared_ptr<T> Allocate(Args&& ... args) {
        ABT_mutex_spinlock(ABT_MUTEX_MEMORY_GET_HANDLE(&s_mtx_mem));
        auto chunk = GetChunkWithFreeSpace();
        auto index = GetFreeSpaceInChunk(chunk.get());
        chunk->m_is_used.set(index);
        ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&s_mtx_mem));
        constexpr static auto deleter = [](T* obj) {
            if(!obj) return;
            obj->~T();
            auto entry = reinterpret_cast<Entry*>(obj);
            auto index = entry->m_index;
            auto chunk = entry->m_owner;
            entry->m_index = -1;
            entry->m_owner = nullptr;
            ABT_mutex_spinlock(ABT_MUTEX_MEMORY_GET_HANDLE(&s_mtx_mem));
            chunk->m_is_used.reset(index);
            ABT_mutex_unlock(ABT_MUTEX_MEMORY_GET_HANDLE(&s_mtx_mem));
        };
        void* location = static_cast<void*>(&chunk->m_content[index]);
        Entry* obj = new (location) Entry(chunk.get(), index, std::forward<Args>(args)...);
        return std::shared_ptr<T>{obj, deleter};
    }
};

}

#endif
