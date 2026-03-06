#include "userfs.h"
#include <cstddef>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <algorithm>

enum {
    BLOCK_SIZE = 512,
    MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
    char memory[BLOCK_SIZE];

    block() { 
		std::memset(memory, 0, BLOCK_SIZE); 
	}
};

struct file {
    std::string name;
    size_t size = 0;
    int refs = 0;
    bool is_deleted = false;
    
    /** 
	* Better performance compared to rlist in struct block.
	* More allocations and memory consumption though.
	*/
    std::vector<block*> blocks;

    ~file() {
        for (block* b : blocks) {
            delete b;
        }
    }
};

struct filedesc {
    file *atfile;
    size_t pos = 0;
    int flags = 0;
};

/** 
* O(1) search by a name, compared to O(N) with rlist,
* more allocations and memory consumption though.
*/
static std::unordered_map<std::string, file*> all_files;

static std::vector<filedesc*> file_descriptors;

static bool is_invalid_fd(int fd) {
    return fd <= 0 || fd >= (int)file_descriptors.size() || file_descriptors[fd] == nullptr;
}

enum ufs_error_code ufs_errno() {
    return ufs_error_code;
}


int ufs_open(const char *filename, int flags) {
    if (!filename) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    file *target = nullptr;
    auto it = all_files.find(filename);

    if (it == all_files.end()) {
        if (!(flags & UFS_CREATE)) {
            ufs_error_code = UFS_ERR_NO_FILE;
            return -1;
        }
        target = new file();
        target->name = filename;
        all_files[filename] = target;
    } else {
        target = it->second;
    }

    filedesc *desc = new filedesc{target, 0, flags};
    target->refs++;

    int fd = -1;

    /** FD 0 reserved */
    if (file_descriptors.empty()) file_descriptors.push_back(nullptr);

    for (size_t i = 1; i < file_descriptors.size(); ++i) {
        if (file_descriptors[i] == nullptr) {
            file_descriptors[i] = desc;
            fd = i;
            break;
        }
    }

    if (fd == -1) {
        file_descriptors.push_back(desc);
        fd = file_descriptors.size() - 1;
    }

    ufs_error_code = UFS_ERR_NO_ERR;
    return fd;
}


ssize_t ufs_write(int fd, const char *buf, size_t size) {
    if (is_invalid_fd(fd)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd];
    if ((desc->flags & UFS_READ_ONLY) && !(desc->flags & UFS_WRITE_ONLY)) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    if (desc->pos + size > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    file *f = desc->atfile;

    size_t needed_blocks = (desc->pos + size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    while (f->blocks.size() < needed_blocks) {
        f->blocks.push_back(new block());
    }

    size_t written = 0;
    while (written < size) {
        size_t block_idx = desc->pos / BLOCK_SIZE;
        size_t offset = desc->pos % BLOCK_SIZE;
        size_t to_write = std::min(size - written, (size_t)BLOCK_SIZE - offset);

        std::memcpy(f->blocks[block_idx]->memory + offset, buf + written, to_write);

        desc->pos += to_write;
        written += to_write;
        f->size = std::max(f->size, desc->pos);
    }

    ufs_error_code = UFS_ERR_NO_ERR;
    return written;
}


ssize_t ufs_read(int fd, char *buf, size_t size) {
    if (is_invalid_fd(fd)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd];
    if ((desc->flags & UFS_WRITE_ONLY) && !(desc->flags & UFS_READ_ONLY)) {
        ufs_error_code = UFS_ERR_NO_PERMISSION;
        return -1;
    }

    file *f = desc->atfile;
    if (desc->pos >= f->size || size == 0) return 0;

    size_t to_read_total = std::min(size, f->size - desc->pos);
    size_t read_bytes = 0;

    while (read_bytes < to_read_total) {
        size_t block_idx = desc->pos / BLOCK_SIZE;
        size_t offset = desc->pos % BLOCK_SIZE;
        size_t to_read = std::min(to_read_total - read_bytes, (size_t)BLOCK_SIZE - offset);

        std::memcpy(buf + read_bytes, f->blocks[block_idx]->memory + offset, to_read);

        desc->pos += to_read;
        read_bytes += to_read;
    }

    ufs_error_code = UFS_ERR_NO_ERR;
    return read_bytes;
}


int ufs_close(int fd) {
    if (is_invalid_fd(fd)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd];
    file *f = desc->atfile;

    f->refs--;
    if (f->refs == 0 && f->is_deleted) {
        delete f;
    }

    delete desc;
    file_descriptors[fd] = nullptr;
    return 0;
}


int ufs_delete(const char *filename) {
    auto it = all_files.find(filename);
    if (it == all_files.end()) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    file *f = it->second;
    all_files.erase(it);
    f->is_deleted = true;

    if (f->refs == 0) {
        delete f;
    }

    return 0;
}


#if NEED_RESIZE
int ufs_resize(int fd, size_t new_size) {
    if (is_invalid_fd(fd)) {
        ufs_error_code = UFS_ERR_NO_FILE;
        return -1;
    }

    filedesc *desc = file_descriptors[fd];
    file *f = desc->atfile;

    if (new_size > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM;
        return -1;
    }

    size_t needed_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (new_size == 0) needed_blocks = 0;

    while (f->blocks.size() < needed_blocks) {
        f->blocks.push_back(new block());
    }
    while (f->blocks.size() > needed_blocks) {
        delete f->blocks.back();
        f->blocks.pop_back();
    }

    f->size = new_size;

    for (filedesc *d : file_descriptors) {
        if (d && d->atfile == f) {
            d->pos = std::min(d->pos, f->size);
        }
    }

    return 0;
}
#endif


void ufs_destroy(void) {
    for (filedesc *desc : file_descriptors) {
        if (desc) {
            desc->atfile->refs--;
            delete desc;
        }
    }

	std::vector<filedesc*> tmp;
	std::swap(tmp, file_descriptors);

    for (auto& [name, f] : all_files) {
        delete f;
    }

    std::unordered_map<std::string, file*> mtmp;
    std::swap(mtmp, all_files);
}
