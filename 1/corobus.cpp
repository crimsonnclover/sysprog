#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <queue>
#include <stdlib.h>
#include <string.h>
#include <vector>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	std::queue<unsigned> data;
	/** For safe exit when the channel is closed */
	bool is_closed;
};

struct coro_bus {
	/** vector stores channels and stores count itself */
	std::vector<struct coro_bus_channel*> channels;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
	return new coro_bus();
}

void
coro_bus_delete(struct coro_bus *bus)
{
	for (size_t i = 0; i < bus->channels.size(); ++i) {
		coro_bus_channel_close(bus, i);
	}
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	coro_bus_channel* channel = new coro_bus_channel();
	channel->size_limit = size_limit;
	channel->is_closed = false;
	rlist_create(&channel->send_queue.coros);
	rlist_create(&channel->recv_queue.coros);

	/* searching for free channel */
	for (size_t i = 0; i < bus->channels.size(); ++i) {
		if (bus->channels[i] == nullptr) { 
			bus->channels[i] = channel;
			return (int)i;
		}
	}
	/* if can't find, creating new one */
	bus->channels.push_back(channel);
	return (int)(bus->channels.size() - 1);
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	if (channel < 0 || channel >= (int)bus->channels.size() || bus->channels[channel] == nullptr) {
		return;
	}

	coro_bus_channel* ch = bus->channels[channel];
	ch->is_closed = true;
	struct wakeup_entry *item, *tmp;
    
	/* 
		Wake up all the waiting coroutines. 
		The is_closed flag will be processed correctly, 
		and they'll all exit with an error.
	*/
    rlist_foreach_entry_safe(item, &ch->send_queue.coros, base, tmp) {
        coro_wakeup(item->coro);
    }
    
    rlist_foreach_entry_safe(item, &ch->recv_queue.coros, base, tmp) {
        coro_wakeup(item->coro);
    }
	/* 
		coro_yield() puts the coroutine at the very end of the list in the scheduler.
		When the scheduler returns to this code, 
		all coroutines will have woken up and exited their send/recv functions, 
		and the channel can be safely deleted.
	*/
	coro_yield();
	delete ch;
	bus->channels[channel] = nullptr;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (channel < 0 || channel >= (int)bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	while (true) {
		if (ch->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		int result = coro_bus_try_send(bus, channel, data);
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			wakeup_entry we;
			we.coro = coro_this();
			rlist_create(&we.base);
			rlist_add_tail(&ch->send_queue.coros, &we.base);
			coro_suspend();
			rlist_del(&we.base);
			continue;
		}

		if (result == 0) {
			if (!rlist_empty(&ch->send_queue.coros) && ch->data.size() < ch->size_limit) {
        		wakeup_entry *sender = rlist_first_entry(&ch->send_queue.coros, wakeup_entry, base);
        		coro_wakeup(sender->coro);
    		}
			return 0;
		}
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	if (ch->size_limit == ch->data.size()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	ch->data.push(data);

    if (!rlist_empty(&ch->recv_queue.coros)) {
        wakeup_entry *rec = rlist_first_entry(&ch->recv_queue.coros, wakeup_entry, base);
        coro_wakeup(rec->coro);
    }

	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	while (true) {
		if (ch->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		int result = coro_bus_try_recv(bus, channel, data);
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			wakeup_entry we;
			we.coro = coro_this();
			rlist_create(&we.base);
			rlist_add_tail(&ch->recv_queue.coros, &we.base);
			coro_suspend();
			rlist_del(&we.base);
			continue;
		}

		if (result == 0) {
			if (!rlist_empty(&ch->recv_queue.coros) && ch->data.size() > 0) {
        		wakeup_entry *rec = rlist_first_entry(&ch->recv_queue.coros, wakeup_entry, base);
        		coro_wakeup(rec->coro);
    		}
			return 0;
		}
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	if (ch->data.size() == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	*data = ch->data.front();
	ch->data.pop();

	if (!rlist_empty(&ch->send_queue.coros)) {
        wakeup_entry *sender = rlist_first_entry(&ch->send_queue.coros, wakeup_entry, base);
        coro_wakeup(sender->coro);
	}

	return 0;
}


#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	while (true) {
		int result = coro_bus_try_broadcast(bus, data);
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		/*
			We are only wating in one coroutine, this is more efficient and easier. 
			Regardless of how the coroutine is awakened — by channel deletion or the appearance of space,
			it works identically: it attempts to write to all open channels
		*/
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			coro_bus_channel* blocking_ch = nullptr;
			for (coro_bus_channel* ch : bus->channels) {
				if (ch != nullptr && !ch->is_closed && ch->size_limit == ch->data.size()) {
					blocking_ch = ch;
					break;
				}
			}
			if (blocking_ch == nullptr) return -1;

			wakeup_entry we;
			we.coro = coro_this();
			rlist_create(&we.base);
			rlist_add_tail(&blocking_ch->send_queue.coros, &we.base);
			coro_suspend();
			rlist_del(&we.base);
			continue;
		}

		if (result == 0) {
			for (coro_bus_channel* ch : bus->channels) {
				if (ch != nullptr && !rlist_empty(&ch->send_queue.coros) && ch->data.size() < ch->size_limit) {
					wakeup_entry *sender = rlist_first_entry(&ch->send_queue.coros, wakeup_entry, base);
					coro_wakeup(sender->coro);
				}
			}
			return 0;
		}
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
    bool has_alive_channels = false;
    
    for (coro_bus_channel* ch : bus->channels) {
        if (ch == nullptr || ch->is_closed) continue;

        has_alive_channels = true;

        if (ch->data.size() >= ch->size_limit) {
            coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
            return -1;
        }
    }

    if (!has_alive_channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    for (coro_bus_channel* ch : bus->channels) {
        if (ch != nullptr && !ch->is_closed) {
            ch->data.push(data);
            if (!rlist_empty(&ch->recv_queue.coros)) {
                wakeup_entry *rec = rlist_first_entry(&ch->recv_queue.coros, wakeup_entry, base);
                coro_wakeup(rec->coro);
            }
        }
    }
    return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	while (true) {
		if (ch->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		int result = coro_bus_try_send_v(bus, channel, data, count);
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			wakeup_entry we;
			we.coro = coro_this();
			rlist_create(&we.base);
			rlist_add_tail(&ch->send_queue.coros, &we.base);
			coro_suspend();
			rlist_del(&we.base);
			continue;
		}

		if (result >= 0) {
			if (!rlist_empty(&ch->send_queue.coros) && ch->data.size() < ch->size_limit) {
        		wakeup_entry *sender = rlist_first_entry(&ch->send_queue.coros, wakeup_entry, base);
        		coro_wakeup(sender->coro);
    		}
			return result;
		}
	}
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];
	if (ch->size_limit == ch->data.size()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	unsigned send_c = 0;
	while (send_c < count && ch->data.size() != ch->size_limit) {
		ch->data.push(data[send_c]);
		send_c++;
	}

	if (!rlist_empty(&ch->recv_queue.coros)) {
        wakeup_entry *rec = rlist_first_entry(&ch->recv_queue.coros, wakeup_entry, base);
        coro_wakeup(rec->coro);
    }

	return send_c;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];

	while (true) {
		if (ch->is_closed) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}

		int result = coro_bus_try_recv_v(bus, channel, data, capacity);
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL) {
			return -1;
		}
		if (result == -1 && coro_bus_errno() == CORO_BUS_ERR_WOULD_BLOCK) {
			wakeup_entry we;
			we.coro = coro_this();
			rlist_create(&we.base);
			rlist_add_tail(&ch->recv_queue.coros, &we.base);
			coro_suspend();
			rlist_del(&we.base);
			continue;
		}

		if (result >= 0) {
			if (!rlist_empty(&ch->recv_queue.coros) && ch->data.size() > 0) {
        		wakeup_entry *rec = rlist_first_entry(&ch->recv_queue.coros, wakeup_entry, base);
        		coro_wakeup(rec->coro);
    		}
			return result;
		}
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (channel < 0 || (size_t)channel >= bus->channels.size() || bus->channels[channel] == nullptr) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}

	coro_bus_channel* ch = bus->channels[channel];
	if (ch->data.size() == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}

	unsigned recv_c = 0;
	while (recv_c < capacity && ch->data.size() != 0) {
		data[recv_c] = ch->data.front();
		ch->data.pop();
		recv_c++;
	}

	if (!rlist_empty(&ch->send_queue.coros)) {
        wakeup_entry *sender = rlist_first_entry(&ch->send_queue.coros, wakeup_entry, base);
        coro_wakeup(sender->coro);
    }

	return recv_c;	
}

#endif
