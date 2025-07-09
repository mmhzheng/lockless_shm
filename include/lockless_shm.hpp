#pragma once
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <string>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <typeinfo>

namespace lockless_shm {

struct alignas(64) ShmHeader {
    size_t head;
    char pad1[64 - sizeof(size_t)];
    size_t tail;
    char pad2[64 - sizeof(size_t)];
    size_t bucket_count;
    size_t data_size;
    char type_name[64];
    char pad3[64 - ((sizeof(size_t) * 2 + 64) % 64)];
};

template<typename T>
struct ShmSlot {
    size_t sequence;
    T data;
};

template<typename T>
class ShmProducer {
public:
    ShmProducer(const std::string& shm_name, size_t bucket_count);
    ~ShmProducer();
    bool create_or_attach();
    int enqueue(const T& item);
    void detach();
private:
    ShmHeader* header_ = nullptr;
    ShmSlot<T>* data_ = nullptr;
    size_t bucket_count_ = 0;
    int shm_fd_ = -1;
    void* shm_addr_ = nullptr;
    bool owner_ = false;
    std::string shm_name_;
};

template<typename T>
class ShmConsumer {
public:
    ShmConsumer(const std::string& shm_name);
    ~ShmConsumer();
    int attach(int timeout_ms = 1000);
    int dequeue(T& item);
    void detach();
private:
    ShmHeader* header_ = nullptr;
    ShmSlot<T>* data_ = nullptr;
    size_t bucket_count_ = 0;
    int shm_fd_ = -1;
    void* shm_addr_ = nullptr;
    std::string shm_name_;
};

constexpr int SHM_ATTACH_OK = 0;
constexpr int SHM_ATTACH_TIMEOUT = 1;
constexpr int SHM_ATTACH_ERROR = 2;
constexpr int ENQUEUE_OK = 0;
constexpr int ENQUEUE_FULL = 1;
constexpr int ENQUEUE_ERR = 2;
constexpr int DEQUEUE_OK = 0;
constexpr int DEQUEUE_EMPTY = 1;
constexpr int DEQUEUE_ERR = 2;

// 工具函数
inline int open_shm(const std::string& name, size_t size, bool create) {
    int flags = O_RDWR | (create ? O_CREAT : 0);
    int fd = shm_open(name.c_str(), flags, 0666);
    if (fd == -1) throw std::runtime_error("shm_open failed");
    if (create) {
        if (ftruncate(fd, size) == -1) {
            close(fd);
            throw std::runtime_error("ftruncate failed");
        }
    }
    return fd;
}

inline void* map_shm(int fd, size_t size) {
    void* addr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) throw std::runtime_error("mmap failed");
    return addr;
}

// ShmProducer 实现
template<typename T>
ShmProducer<T>::ShmProducer(const std::string& shm_name, size_t bucket_count)
    : header_(nullptr), data_(nullptr), bucket_count_(bucket_count), shm_fd_(-1), shm_addr_(nullptr), owner_(false), shm_name_(shm_name) {}

template<typename T>
ShmProducer<T>::~ShmProducer() { detach(); }

template<typename T>
void ShmProducer<T>::detach() {
    if (shm_addr_) {
        size_t shm_size = sizeof(ShmHeader) + bucket_count_ * sizeof(ShmSlot<T>);
        munmap(shm_addr_, shm_size);
        shm_addr_ = nullptr;
    }
    if (shm_fd_ != -1) {
        close(shm_fd_);
        shm_fd_ = -1;
    }
}

template<typename T>
bool ShmProducer<T>::create_or_attach() {
    size_t shm_size = sizeof(ShmHeader) + bucket_count_ * sizeof(ShmSlot<T>);
    int fd = -1;
    void* addr = nullptr;
    bool created = false;
    fd = shm_open(shm_name_.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666);
    if (fd != -1) {
        if (ftruncate(fd, shm_size) == -1) {
            close(fd);
            throw std::runtime_error("ftruncate failed");
        }
        addr = mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed");
        }
        header_ = reinterpret_cast<ShmHeader*>(addr);
        data_ = reinterpret_cast<ShmSlot<T>*>(reinterpret_cast<char*>(addr) + sizeof(ShmHeader));
        memset(header_, 0, sizeof(ShmHeader));
        header_->bucket_count = bucket_count_;
        header_->data_size = sizeof(T);
        strncpy(header_->type_name, typeid(T).name(), 64 - 1);
        header_->type_name[64 - 1] = '\0';
        for (size_t i = 0; i < bucket_count_; ++i) {
            data_[i].sequence = i;
        }
        created = true;
        owner_ = true;
    } else {
        fd = shm_open(shm_name_.c_str(), O_RDWR, 0666);
        if (fd == -1) throw std::runtime_error("shm_open attach failed");
        addr = mmap(nullptr, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (addr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("mmap failed");
        }
        header_ = reinterpret_cast<ShmHeader*>(addr);
        data_ = reinterpret_cast<ShmSlot<T>*>(reinterpret_cast<char*>(addr) + sizeof(ShmHeader));
        owner_ = false;
    }
    shm_fd_ = fd;
    shm_addr_ = addr;
    return created;
}

template<typename T>
int ShmProducer<T>::enqueue(const T& item) {
    size_t capacity = bucket_count_;
    while (true) {
        size_t pos = header_->head;
        ShmSlot<T>* slot = &data_[pos % capacity];
        size_t seq = __sync_fetch_and_add(&slot->sequence, 0);
        intptr_t diff = (intptr_t)seq - (intptr_t)pos;
        if (diff == 0) {
            if (__sync_bool_compare_and_swap(&header_->head, pos, pos + 1)) {
                slot->data = item;
                __sync_lock_test_and_set(&slot->sequence, pos + 1);
                return ENQUEUE_OK;
            } else {
                std::this_thread::yield();
            }
        } else if (diff < 0) {
            return ENQUEUE_FULL;
        } else {
            std::this_thread::yield();
        }
    }
}

// ShmConsumer 实现
template<typename T>
ShmConsumer<T>::ShmConsumer(const std::string& shm_name)
    : header_(nullptr), data_(nullptr), bucket_count_(0), shm_fd_(-1), shm_addr_(nullptr), shm_name_(shm_name) {}

template<typename T>
ShmConsumer<T>::~ShmConsumer() { detach(); }

template<typename T>
void ShmConsumer<T>::detach() {
    if (shm_addr_) {
        size_t shm_size = sizeof(ShmHeader) + bucket_count_ * sizeof(ShmSlot<T>);
        munmap(shm_addr_, shm_size);
        shm_addr_ = nullptr;
    }
    if (shm_fd_ != -1) {
        close(shm_fd_);
        shm_fd_ = -1;
    }
}

template<typename T>
int ShmConsumer<T>::attach(int timeout_ms) {
    using namespace std::chrono;
    auto start = steady_clock::now();
    while (true) {
        int fd = -1;
        void* addr = nullptr;
        try {
            fd = open_shm(shm_name_, sizeof(ShmHeader), false);
            addr = map_shm(fd, sizeof(ShmHeader));
        } catch (...) {
            if (timeout_ms > 0 && duration_cast<milliseconds>(steady_clock::now() - start).count() > timeout_ms) {
                return SHM_ATTACH_TIMEOUT;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        ShmHeader* header = reinterpret_cast<ShmHeader*>(addr);
        if (header->bucket_count == 0) {
            munmap(addr, sizeof(ShmHeader));
            close(fd);
            if (timeout_ms > 0 && duration_cast<milliseconds>(steady_clock::now() - start).count() > timeout_ms) {
                return SHM_ATTACH_TIMEOUT;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        bucket_count_ = header->bucket_count;
        size_t shm_size = sizeof(ShmHeader) + bucket_count_ * sizeof(ShmSlot<T>);
        munmap(addr, sizeof(ShmHeader));
        close(fd);
        try {
            shm_fd_ = open_shm(shm_name_, shm_size, false);
            shm_addr_ = map_shm(shm_fd_, shm_size);
        } catch (...) {
            return SHM_ATTACH_ERROR;
        }
        header_ = reinterpret_cast<ShmHeader*>(shm_addr_);
        data_ = reinterpret_cast<ShmSlot<T>*>(reinterpret_cast<char*>(shm_addr_) + sizeof(ShmHeader));
        return SHM_ATTACH_OK;
    }
}

template<typename T>
int ShmConsumer<T>::dequeue(T& item) {
    size_t capacity = bucket_count_;
    while (true) {
        size_t pos = header_->tail;
        ShmSlot<T>* slot = &data_[pos % capacity];
        size_t seq = __sync_fetch_and_add(&slot->sequence, 0);
        intptr_t diff = (intptr_t)seq - (intptr_t)(pos + 1);
        if (diff == 0) {
            if (__sync_bool_compare_and_swap(&header_->tail, pos, pos + 1)) {
                item = slot->data;
                __sync_lock_test_and_set(&slot->sequence, pos + capacity);
                return DEQUEUE_OK;
            } else {
                std::this_thread::yield();
            }
        } else if (diff < 0) {
            return DEQUEUE_EMPTY;
        } else {
            std::this_thread::yield();
        }
    }
}

} // namespace lockless_shm
