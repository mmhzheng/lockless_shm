#include "lockless_shm.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <cstring>
#include <sys/wait.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <set>
#include <algorithm>
#include <csignal>
#include <sys/mman.h>
#include <ctime> // Added for time(nullptr)
#include <dirent.h> // Added for opendir/readdir

struct MyData {
    int id;
    char msg[32];
};

using namespace lockless_shm;

constexpr const char* SHM_NAME = "/lockless_demo";
constexpr size_t BUCKETS = 1024;
constexpr int ATTACH_TIMEOUT_MS = 3000;

constexpr const char* STAT_SHM_NAME = "/lockless_stat";

// 1. 统计结构体 stop_flag 用 int 替代 bool
struct StatShm {
    alignas(64) size_t produced;
    char pad1[64 - sizeof(size_t)];
    alignas(64) size_t consumed;
    char pad2[64 - sizeof(size_t)];
    alignas(64) int stop_flag; // 0: 继续, 1: 生产者停止, 2: 消费者停止
    char pad3[64 - sizeof(int)];
};

// 工具函数：等待所有子进程
void wait_all_children(const std::vector<pid_t>& pids) {
    for (pid_t pid : pids) {
        int status = 0;
        waitpid(pid, &status, 0);
    }
}

// 创建/映射统计用共享内存
StatShm* create_stat_shm(bool create) {
    int flags = O_RDWR | (create ? O_CREAT : 0);
    int fd = shm_open(STAT_SHM_NAME, flags, 0666);
    if (fd == -1) {
        perror("shm_open stat");
        std::cerr << "[DEBUG] shm_open failed, create=" << create << std::endl << std::flush;
        exit(1);
    }
    if (create) {
        if (ftruncate(fd, sizeof(StatShm)) == -1) {
            perror("ftruncate stat");
            std::cerr << "[DEBUG] ftruncate failed" << std::endl << std::flush;
            exit(1);
        }
    }
    void* addr = mmap(nullptr, sizeof(StatShm), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        perror("mmap stat");
        std::cerr << "[DEBUG] mmap failed" << std::endl << std::flush;
        exit(1);
    }
    close(fd);
    std::cerr << "[DEBUG] create_stat_shm success, create=" << create << std::endl << std::flush;
    return reinterpret_cast<StatShm*>(addr);
}

void unlink_stat_shm() {
    shm_unlink(STAT_SHM_NAME);
}

// 修改吞吐量测试接口和main参数解析
// test1 <producers> <consumers> <duration_sec>
// test2 <producers> <consumers> <items_per_producer>

// 修改吞吐量测试：生产者和消费者无限生产/消费，直到stop_flag=1
void run_throughput_test(int n_producers, int n_consumers, int duration_sec) {
    shm_unlink(SHM_NAME);
    unlink_stat_shm();
    StatShm* stat = create_stat_shm(true);
    stat->produced = 0;
    stat->consumed = 0;
    stat->stop_flag = 0;
    // 初始化shm
    pid_t init_pid = fork();
    if (init_pid == 0) {
        ShmProducer<MyData> prod(SHM_NAME, BUCKETS);
        prod.create_or_attach();
        prod.detach();
        _exit(0);
    } else {
        waitpid(init_pid, nullptr, 0);
    }
    std::vector<pid_t> producer_pids;
    std::vector<pid_t> consumer_pids;
    // 生产者进程
    for (int i = 0; i < n_producers; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            StatShm* stat = create_stat_shm(false);
            ShmProducer<MyData> prod(SHM_NAME, BUCKETS);
            prod.create_or_attach();
            int val = i * 100000000;
            while (__sync_fetch_and_add(&stat->stop_flag, 0) == 0) {
                MyData data;
                data.id = val;
                snprintf(data.msg, sizeof(data.msg), "msg%d", val);
                if (prod.enqueue(data) == ENQUEUE_OK) {
                    __sync_fetch_and_add(&stat->produced, 1);
                    ++val;
                } else {
                    if (__sync_fetch_and_add(&stat->stop_flag, 0) == 1) break;
                    std::this_thread::yield();
                }
                if (__sync_fetch_and_add(&stat->stop_flag, 0) == 1) break;
            }
            std::cout << "[Producer] PID " << getpid() << " exiting at " << time(nullptr) << ", produced: " << (val - i * 100000000) << std::endl;
            prod.detach();
            _exit(0);
        } else {
            producer_pids.push_back(pid);
        }
    }
    // 消费者进程
    for (int i = 0; i < n_consumers; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            std::cout << "[Consumer] PID " << getpid() << " started at " << time(nullptr) << std::endl;
            StatShm* stat = create_stat_shm(false);
            ShmConsumer<MyData> cons(SHM_NAME);
            if (cons.attach(ATTACH_TIMEOUT_MS) != SHM_ATTACH_OK) {
                std::cout << "[Consumer] PID " << getpid() << " attach failed, exiting at " << time(nullptr) << std::endl;
                _exit(1);
            }
            MyData value;
            size_t consumed_count = 0;
            while (true) {
                if (cons.dequeue(value) == DEQUEUE_OK) {
                    __sync_fetch_and_add(&stat->consumed, 1);
                    ++consumed_count;
                    // 可选：打印收到的数据内容
                    // std::cout << "[Consumer] got id=" << value.id << ", msg=" << value.msg << std::endl;
                } else {
                    if (__sync_fetch_and_add(&stat->stop_flag, 0) == 2) break;
                    std::this_thread::yield();
                }
            }
            std::cout << "[Consumer] PID " << getpid() << " exiting at " << time(nullptr) << ", consumed: " << consumed_count << std::endl;
            cons.detach();
            _exit(0);
        } else {
            consumer_pids.push_back(pid);
        }
    }
    // 主进程负责sleep和stop_flag控制
    size_t last_cons = 0;
    for (int t = 0; t < duration_sec; ++t) {
        sleep(1);
        size_t cur_prod = __sync_fetch_and_add(&stat->produced, 0);
        size_t cur_cons = __sync_fetch_and_add(&stat->consumed, 0);
        int alive = 0;
        for (size_t i = 0; i < producer_pids.size() + consumer_pids.size(); ++i) {
            pid_t pid = (i < producer_pids.size()) ? producer_pids[i] : consumer_pids[i - producer_pids.size()];
            if (waitpid(pid, nullptr, WNOHANG) == 0) ++alive;
        }
        std::cout << "[Monitor] Time: " << (t+1) << "s, Produced: " << cur_prod << ", Consumed: " << cur_cons
                  << ", QPS: " << (cur_cons - last_cons) << ", Alive children: " << alive << std::endl;
        last_cons = cur_cons;
    }
    // 先通知生产者退出
    __sync_lock_test_and_set(&stat->stop_flag, 1);
    // 等待所有生产者退出
    wait_all_children(producer_pids);
    // 再通知消费者退出
    __sync_lock_test_and_set(&stat->stop_flag, 2);
    // 等待所有消费者退出
    wait_all_children(consumer_pids);
    std::cout << "[Throughput Test] Final Produced: " << __sync_fetch_and_add(&stat->produced, 0)
              << ", Final Consumed: " << __sync_fetch_and_add(&stat->consumed, 0) << std::endl;
    munmap(stat, sizeof(StatShm));
    shm_unlink(SHM_NAME);
    unlink_stat_shm();
}

// 多进程一致性校验（持续 duration_sec 秒，统计每个进程明细）
void run_consistency_test(int n_producers, int n_consumers, int duration_sec) {
    std::cout << "[DEBUG] run_consistency_test start" << std::endl << std::flush;
    shm_unlink(SHM_NAME);
    unlink_stat_shm();
    StatShm* stat = create_stat_shm(true);
    stat->produced = 0;
    stat->consumed = 0;
    stat->stop_flag = 0;
    // 初始化shm
    pid_t init_pid = fork();
    if (init_pid == 0) {
        std::cout << "[DEBUG] init producer process started" << std::endl << std::flush;
        ShmProducer<MyData> prod(SHM_NAME, BUCKETS);
        prod.create_or_attach();
        prod.detach();
        std::cout << "[DEBUG] init producer process exiting" << std::endl << std::flush;
        _exit(0);
    } else {
        waitpid(init_pid, nullptr, 0);
        std::cout << "[DEBUG] init producer process finished" << std::endl << std::flush;
    }
    std::vector<pid_t> producer_pids;
    std::vector<pid_t> consumer_pids;
    // 启动生产者进程
    for (int i = 0; i < n_producers; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            std::cout << "[DEBUG] Producer PID " << getpid() << " starting" << std::endl << std::flush;
            StatShm* stat = create_stat_shm(false);
            ShmProducer<MyData> prod(SHM_NAME, BUCKETS);
            prod.create_or_attach();
            int val = i * 100000000;
            size_t produced_count = 0;
            std::vector<MyData> produced_data;
            std::string fname = "/tmp/producer_" + std::to_string(getpid()) + ".dat";
            sleep(1);
            while (__sync_fetch_and_add(&stat->stop_flag, 0) == 0) {
                MyData data;
                data.id = val;
                snprintf(data.msg, sizeof(data.msg), "msg%d", val);
                while (prod.enqueue(data) != ENQUEUE_OK) {
                    if (__sync_fetch_and_add(&stat->stop_flag, 0) == 1) break;
                    std::this_thread::yield();
                }
                if (__sync_fetch_and_add(&stat->stop_flag, 0) == 1) break;
                produced_data.push_back(data);
                ++val;
                ++produced_count;
                __sync_fetch_and_add(&stat->produced, 1);
            }
            std::ofstream fout(fname, std::ios::binary);
            for (size_t i = 0; i < produced_data.size(); ++i) {
                fout.write(reinterpret_cast<const char*>(&produced_data[i]), sizeof(MyData));
            }
            fout.close();
            std::cout << "[Producer] PID " << getpid() << " exiting, produced: " << produced_count << std::endl;
            prod.detach();
            _exit(0);
        } else {
            producer_pids.push_back(pid);
        }
    }
    // 启动消费者进程
    for (int i = 0; i < n_consumers; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            std::cout << "[DEBUG] Consumer PID " << getpid() << " starting" << std::endl << std::flush;
            StatShm* stat = create_stat_shm(false);
            ShmConsumer<MyData> cons(SHM_NAME);
            int attach_result = cons.attach(ATTACH_TIMEOUT_MS);
            if (attach_result != SHM_ATTACH_OK) {
                std::cerr << "[Consumer] PID " << getpid() << " attach failed, result: " << attach_result << std::endl << std::flush;
                _exit(1);
            }
            std::string fname = "/tmp/consumer_" + std::to_string(getpid()) + ".dat";
            std::vector<MyData> consumed_data;
            size_t consumed_count = 0;
            MyData value;
            while (__sync_fetch_and_add(&stat->stop_flag, 0) == 0) {
                if (cons.dequeue(value) == DEQUEUE_OK) {
                    consumed_data.push_back(value);
                    ++consumed_count;
                    __sync_fetch_and_add(&stat->consumed, 1);
                } else {
                    std::this_thread::yield();
                }
            }
            std::ofstream fout(fname, std::ios::binary);
            for (size_t i = 0; i < consumed_data.size(); ++i) {
                fout.write(reinterpret_cast<const char*>(&consumed_data[i]), sizeof(MyData));
            }
            fout.close();
            std::cout << "[Consumer] PID " << getpid() << " exiting, consumed: " << consumed_count << std::endl;
            cons.detach();
            _exit(0);
        } else {
            consumer_pids.push_back(pid);
        }
    }
    std::cout << "[DEBUG] All producers/consumers started" << std::endl << std::flush;
    // 主进程负责sleep和stop_flag控制
    for (int t = 0; t < duration_sec; ++t) {
        sleep(1);
        size_t cur_prod = __sync_fetch_and_add(&stat->produced, 0);
        size_t cur_cons = __sync_fetch_and_add(&stat->consumed, 0);
        int alive = 0;
        for (size_t i = 0; i < producer_pids.size() + consumer_pids.size(); ++i) {
            pid_t pid = (i < producer_pids.size()) ? producer_pids[i] : consumer_pids[i - producer_pids.size()];
            if (waitpid(pid, nullptr, WNOHANG) == 0) ++alive;
        }
        std::cout << "[Monitor] Time: " << (t+1) << "s, Produced: " << cur_prod << ", Consumed: " << cur_cons
                  << ", Alive children: " << alive << std::endl << std::flush;
    }
    // 先通知生产者退出
    __sync_lock_test_and_set(&stat->stop_flag, 1);
    // 等待所有生产者退出
    wait_all_children(producer_pids);
    // 再通知消费者退出
    __sync_lock_test_and_set(&stat->stop_flag, 2);
    // 等待所有消费者退出
    wait_all_children(consumer_pids);
    // 汇总所有生产/消费数据
    std::set<int> produced, consumed;
    std::vector<std::pair<std::string, size_t>> prod_detail, cons_detail;
    DIR* dirp = opendir("/tmp");
    if (dirp) {
        struct dirent* dp;
        while ((dp = readdir(dirp)) != nullptr) {
            std::string fname = dp->d_name;
            if (fname.find("producer_") == 0 && fname.find(".dat") != std::string::npos) {
                std::string file = std::string("/tmp/") + fname;
                std::ifstream fin(file, std::ios::binary);
                size_t cnt = 0;
                MyData val;
                while (fin.read(reinterpret_cast<char*>(&val), sizeof(MyData))) {
                    produced.insert(val.id);
                    ++cnt;
                }
                fin.close();
                prod_detail.push_back({file, cnt});
            }
        }
        closedir(dirp);
    }
    dirp = opendir("/tmp");
    if (dirp) {
        struct dirent* dp;
        while ((dp = readdir(dirp)) != nullptr) {
            std::string fname = dp->d_name;
            if (fname.find("consumer_") == 0 && fname.find(".dat") != std::string::npos) {
                std::string file = std::string("/tmp/") + fname;
                std::ifstream fin(file, std::ios::binary);
                size_t cnt = 0;
                MyData val;
                while (fin.read(reinterpret_cast<char*>(&val), sizeof(MyData))) {
                    consumed.insert(val.id);
                    ++cnt;
                }
                fin.close();
                cons_detail.push_back({file, cnt});
            }
        }
        closedir(dirp);
    }
    // 打印每个生产者/消费者的明细
    std::cout << "[Detail] Producer counts:" << std::endl;
    for (auto& p : prod_detail) {
        std::cout << "  " << p.first << ": " << p.second << std::endl;
    }
    std::cout << "[Detail] Consumer counts:" << std::endl;
    for (auto& c : cons_detail) {
        std::cout << "  " << c.first << ": " << c.second << std::endl;
    }
    // 检查丢失、重复
    std::vector<int> missing, extra;
    std::set_difference(produced.begin(), produced.end(), consumed.begin(), consumed.end(), std::back_inserter(missing));
    std::set_difference(consumed.begin(), consumed.end(), produced.begin(), produced.end(), std::back_inserter(extra));
    std::cout << "[Consistency Test] Produced unique: " << produced.size() << ", Consumed unique: " << consumed.size()
              << ", Missing: " << missing.size() << ", Extra: " << extra.size() << std::endl;
    if (!missing.empty()) {
        std::cout << "Missing samples: ";
        for (size_t i = 0; i < std::min<size_t>(10, missing.size()); ++i) std::cout << missing[i] << " ";
        std::cout << std::endl;
    }
    if (!extra.empty()) {
        std::cout << "Extra samples: ";
        for (size_t i = 0; i < std::min<size_t>(10, extra.size()); ++i) std::cout << extra[i] << " ";
        std::cout << std::endl;
    }
    // 清理
    int res = system("rm -f /tmp/producer_*.dat /tmp/consumer_*.dat");
    (void)res;
    munmap(stat, sizeof(StatShm));
    shm_unlink(SHM_NAME);
    unlink_stat_shm();
}

// main参数解析调整
enum TestMode { MODE_NONE, MODE_THROUGHPUT, MODE_CONSISTENCY };

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cout << "Usage: " << argv[0] << " test1 <producers> <consumers> <duration_sec>\n"
                  << "       " << argv[0] << " test2 <producers> <consumers> <duration_sec>" << std::endl;
        return 1;
    }
    std::string mode = argv[1];
    if (mode == "test1") {
        if (argc < 5) {
            std::cout << "Usage: " << argv[0] << " test1 <producers> <consumers> <duration_sec>" << std::endl;
            return 1;
        }
        int n_producers = std::stoi(argv[2]);
        int n_consumers = std::stoi(argv[3]);
        int duration_sec = std::stoi(argv[4]);
        run_throughput_test(n_producers, n_consumers, duration_sec);
    } else if (mode == "test2") {
        if (argc < 5) {
            std::cout << "Usage: " << argv[0] << " test2 <producers> <consumers> <duration_sec>" << std::endl;
            return 1;
        }
        int n_producers = std::stoi(argv[2]);
        int n_consumers = std::stoi(argv[3]);
        int duration_sec = std::stoi(argv[4]);
        run_consistency_test(n_producers, n_consumers, duration_sec);
    } else {
        std::cout << "Unknown mode: " << mode << std::endl;
        return 1;
    }
    return 0;
}
