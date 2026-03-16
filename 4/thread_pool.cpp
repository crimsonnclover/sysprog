#include "thread_pool.h"

#include <pthread.h>
#include <queue>
#include <vector>
#include <cmath>
#include <time.h>


enum TaskState {
    TASK_CREATED,
    TASK_PUSHED,
    TASK_RUNNING,
    TASK_FINISHED
};


struct thread_task {
    thread_task_f function;
    TaskState state;
    bool is_detached;
    pthread_mutex_t mutex;
    pthread_cond_t cv;

    thread_task(const thread_task_f& func) : function(func), state(TASK_CREATED), is_detached(false) {
        pthread_mutex_init(&mutex, nullptr);
        pthread_cond_init(&cv, nullptr);
    }

    ~thread_task() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cv);
    }
};

struct thread_pool {
    std::vector<pthread_t> threads;
    std::queue<thread_task*> task_queue;
    
    int max_thread_count;
    int running_tasks;
    bool is_shutting_down;
    
    pthread_mutex_t mutex;
    pthread_cond_t cv;

    thread_pool(int count) : max_thread_count(count), running_tasks(0), is_shutting_down(false) {
        pthread_mutex_init(&mutex, nullptr);
        pthread_cond_init(&cv, nullptr);
    }

    ~thread_pool() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cv);
    }
};

static void* worker_thread_routine(void* arg) {
    thread_pool* pool = static_cast<thread_pool*>(arg);

    while (true) {
        pthread_mutex_lock(&pool->mutex);

        while (pool->task_queue.empty() && !pool->is_shutting_down) {
            pthread_cond_wait(&pool->cv, &pool->mutex);
        }

        if (pool->is_shutting_down && pool->task_queue.empty()) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        thread_task* task = pool->task_queue.front();
        pool->task_queue.pop();
        pool->running_tasks++;
        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->state = TASK_RUNNING;
        pthread_mutex_unlock(&task->mutex);

        task->function();

        pthread_mutex_lock(&task->mutex);
        task->state = TASK_FINISHED;
        bool detached = task->is_detached;
        pthread_cond_broadcast(&task->cv);
        pthread_mutex_unlock(&task->mutex);

        if (detached) {
            delete task;
        }

        pthread_mutex_lock(&pool->mutex);
        pool->running_tasks--;
        pthread_mutex_unlock(&pool->mutex);
    }

    return nullptr;
}

int thread_pool_new(int thread_count, struct thread_pool **pool) {
    if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS) {
        return TPOOL_ERR_INVALID_ARGUMENT;
    }
    
    *pool = new thread_pool(thread_count);
    return 0;
}

int thread_pool_delete(struct thread_pool *pool) {
    pthread_mutex_lock(&pool->mutex);
    if (!pool->task_queue.empty() || pool->running_tasks > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }
    
    pool->is_shutting_down = true;
    pthread_cond_broadcast(&pool->cv);
    pthread_mutex_unlock(&pool->mutex);

    for (pthread_t tid : pool->threads) {
        pthread_join(tid, nullptr);
    }

    delete pool;
    return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    pthread_mutex_lock(&pool->mutex);
    
    if (pool->task_queue.size() + pool->running_tasks >= TPOOL_MAX_TASKS) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_TOO_MANY_TASKS;
    }

    pthread_mutex_lock(&task->mutex);
    task->state = TASK_PUSHED;
    task->is_detached = false;
    pthread_mutex_unlock(&task->mutex);

    pool->task_queue.push(task);

    int idle_threads = pool->threads.size() - pool->running_tasks;
    if (idle_threads == 0 && pool->threads.size() < (size_t)pool->max_thread_count) {
        pthread_t tid;
        pthread_create(&tid, nullptr, worker_thread_routine, pool);
        pool->threads.push_back(tid);
    }

    pthread_cond_signal(&pool->cv);
    pthread_mutex_unlock(&pool->mutex);
    
    return 0;
}

int thread_task_new(struct thread_task **task, const thread_task_f &function) {
    *task = new thread_task(function);
    return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
    pthread_mutex_lock(&const_cast<struct thread_task*>(task)->mutex);
    bool finished = (task->state == TASK_FINISHED);
    pthread_mutex_unlock(&const_cast<struct thread_task*>(task)->mutex);
    return finished;
}

bool thread_task_is_running(const struct thread_task *task) {
    pthread_mutex_lock(&const_cast<struct thread_task*>(task)->mutex);
    bool running = (task->state == TASK_RUNNING);
    pthread_mutex_unlock(&const_cast<struct thread_task*>(task)->mutex);
    return running;
}

int thread_task_join(struct thread_task *task) {
    pthread_mutex_lock(&task->mutex);
    
    if (task->state == TASK_CREATED) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    while (task->state != TASK_FINISHED) {
        pthread_cond_wait(&task->cv, &task->mutex);
    }
    
    pthread_mutex_unlock(&task->mutex);
    return 0;
}

#if NEED_TIMED_JOIN
int thread_task_timed_join(struct thread_task *task, double timeout) {
    pthread_mutex_lock(&task->mutex);
    
    if (task->state == TASK_CREATED) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (timeout <= 0.0) {
        if (task->state != TASK_FINISHED) {
            pthread_mutex_unlock(&task->mutex);
            return TPOOL_ERR_TIMEOUT;
        }
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    
    if (timeout > 1e9) {
        ts.tv_sec += 1e9;
    } else {
        double intpart;
        double fractpart = modf(timeout, &intpart);
        ts.tv_sec += (time_t)intpart;
        ts.tv_nsec += (long)(fractpart * 1e9);
        
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }
    }

    int rc = 0;
    while (task->state != TASK_FINISHED && rc != ETIMEDOUT) {
        rc = pthread_cond_timedwait(&task->cv, &task->mutex, &ts);
    }

    if (task->state != TASK_FINISHED) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TIMEOUT;
    }

    pthread_mutex_unlock(&task->mutex);
    return 0;
}
#endif

int thread_task_delete(struct thread_task *task) {
    pthread_mutex_lock(&task->mutex);
    if (task->state == TASK_PUSHED || task->state == TASK_RUNNING) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }
    pthread_mutex_unlock(&task->mutex);
    
    delete task;
    return 0;
}

#if NEED_DETACH
int thread_task_detach(struct thread_task *task) {
    pthread_mutex_lock(&task->mutex);
    
    if (task->state == TASK_CREATED) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->state == TASK_FINISHED) {
        pthread_mutex_unlock(&task->mutex);
        delete task;
        return 0;
    }

    task->is_detached = true;
    pthread_mutex_unlock(&task->mutex);
    return 0;
}
#endif
