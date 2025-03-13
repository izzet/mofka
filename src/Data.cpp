/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mofka/Data.hpp"
#include "mofka/Exception.hpp"

#include "DataImpl.hpp"
#include "PimplUtil.hpp"

#include <cstring>
#include <iostream>

namespace mofka {

PIMPL_DEFINE_COMMON_FUNCTIONS_NO_CTOR(Data);

Data::Data(Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(ctx, std::move(free_cb))) {}

Data::Data(void* ptr, size_t size, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(ptr, size, ctx, std::move(free_cb))) {}

Data::Data(std::vector<Segment> segments, Context ctx, FreeCallback free_cb)
: self(std::make_shared<DataImpl>(std::move(segments), ctx, std::move(free_cb))) {}

const std::vector<Data::Segment>& Data::segments() const {
    static const std::vector<Data::Segment> no_segments;
    if(self) return self->m_segments;
    else return no_segments;
}

size_t Data::size() const {
    if(self) return self->m_size;
    else return 0;
}

Data::Context Data::context() const {
    if(self) return self->m_context;
    else return nullptr;
}

void Data::write(const char* data, size_t size, size_t offset) const {
    if(!self && (size != 0))
        throw Exception{"Trying to call Data::write on a null Data object"};
    size_t off = 0;
    for(auto& seg : segments()) {
        if(offset >= seg.size) {
            offset -= seg.size;
            continue;
        }
        // current segment needs to be copied
        auto size_to_copy = std::min(size, seg.size);
        std::memcpy((char*)seg.ptr + offset, data + off, size_to_copy);
        offset = 0;
        off += size_to_copy;
        size -= size_to_copy;
        if(size == 0) break;
    }
}

}
