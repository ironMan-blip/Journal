#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define JOURNAL_MAGIC 0X4A524E4CU
#define BLOCK_SIZE        4096U
#define INODE_SIZE         128U
#define JOURNAL_BLOCK_IDX    1U
#define JOURNAL_BLOCKS      16U
#define INODE_BLOCKS         2U
#define DATA_BLOCKS         64U
#define INODE_BMAP_IDX     (JOURNAL_BLOCK_IDX + JOURNAL_BLOCKS)
#define DATA_BMAP_IDX      (INODE_BMAP_IDX + 1U)
#define INODE_START_IDX    (DATA_BMAP_IDX + 1U)
#define DATA_START_IDX     (INODE_START_IDX + INODE_BLOCKS)
#define TOTAL_BLOCKS       (DATA_START_IDX + DATA_BLOCKS)
#define REC_DATA    1
#define REC_COMMIT  2
#define IMG "vsfs.img"

struct superblock {
    uint32_t magic;
    uint32_t block_size;
    uint32_t total_blocks;
    uint32_t inode_count;
    uint32_t journal_block;
    uint32_t inode_bitmap;
    uint32_t data_bitmap;
    uint32_t inode_start;
    uint32_t data_start;
    uint8_t  _pad[128 - 9 * 4];
};

struct inode {
    uint16_t type;
    uint16_t links;
    uint32_t size;
    uint32_t direct[8];
    uint32_t ctime;
    uint32_t mtime;
    uint8_t _pad[128 - (2 + 2 + 4 + 8 * 4 + 4 + 4)];
};

struct dirent {
    uint32_t inode;
    char name[28];
};

struct journal_header { 
    uint32_t magic; 
    uint32_t nbytes_used;
};

struct rec_header {
    uint16_t type;
    uint16_t size;
};

struct data_record {
    struct rec_header hdr;
    uint32_t block_no;
    uint8_t data[4096];
};

struct commit_record {
    struct rec_header hdr;
};

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static void write_block(int fd, uint32_t block_no, void *buf) {
    if (pwrite(fd, buf, BLOCK_SIZE, (off_t)block_no * BLOCK_SIZE) != (ssize_t)BLOCK_SIZE)
        die("write");
}

static void read_block(int fd, uint32_t block_no, void *buf) {
    if (pread(fd, buf, BLOCK_SIZE, (off_t)block_no * BLOCK_SIZE) != (ssize_t)BLOCK_SIZE)
        die("read");
}

static void set_bitmap(uint8_t *bitmap, uint32_t index) {
    bitmap[index / 8] |= (uint8_t)(1U << (index % 8));
}

static int free_inode(uint8_t *bmap){
    uint32_t max_inodes = INODE_BLOCKS * (BLOCK_SIZE / INODE_SIZE);
    for(uint32_t i=0; i<max_inodes; i++){
        if(!(bmap[i/8] & (1 << (i%8)))) return i;
    }
    return -1;
}

static int free_dirent(struct dirent *d){
    int slots = BLOCK_SIZE / sizeof(struct dirent);
    for(int i=0; i<slots; i++){
        if(d[i].name[0] == '\0') return i;
    }
    return -1;
}

static void journal_write(int fd, uint32_t offset, void *buf, uint32_t size) {
    if (pwrite(fd, buf, size, (off_t)JOURNAL_BLOCK_IDX * BLOCK_SIZE + offset) != (ssize_t)size)
        die("journal_write");
}

static void journal_read(int fd, uint32_t offset, void *buf, uint32_t size) {
    if (pread(fd, buf, size, (off_t)JOURNAL_BLOCK_IDX * BLOCK_SIZE + offset) != (ssize_t)size)
        die("journal_read");
}

static void create_journal(const char *file_name) {
    int fd = open(IMG, O_RDWR);
    if(fd < 0) die("open");

    uint8_t ibmap[BLOCK_SIZE], dbmap[BLOCK_SIZE], root_data[BLOCK_SIZE], ino_blk[BLOCK_SIZE];
    
    read_block(fd, INODE_BMAP_IDX, ibmap);
    read_block(fd, DATA_BMAP_IDX, dbmap);
    read_block(fd, DATA_START_IDX, root_data);

    int inode_idx = free_inode(ibmap);
    struct dirent *d = (struct dirent *)root_data;
    int slot = free_dirent(d);

    if(inode_idx < 0 || slot < 0) { close(fd); return; }

    set_bitmap(ibmap, inode_idx);
    d[slot].inode = inode_idx;
    strncpy(d[slot].name, file_name, 27);

    uint32_t iblk_no = INODE_START_IDX + (inode_idx / (BLOCK_SIZE / INODE_SIZE));
    read_block(fd, iblk_no, ino_blk);
    
    struct inode *new_ino = &((struct inode *)ino_blk)[inode_idx % (BLOCK_SIZE / INODE_SIZE)];
    memset(new_ino, 0, sizeof(struct inode));
    new_ino->type = 1; new_ino->links = 1;
    new_ino->ctime = new_ino->mtime = (uint32_t)time(NULL);

    struct inode *root_ino = &((struct inode *)ino_blk)[0]; 
    uint32_t needed = (slot + 1) * sizeof(struct dirent);
    if(root_ino->size < needed) root_ino->size = needed;

    struct journal_header jh;
    journal_read(fd, 0, &jh, sizeof(jh));
    if(jh.magic != JOURNAL_MAGIC) { jh.magic = JOURNAL_MAGIC; jh.nbytes_used = sizeof(jh); }
    uint32_t off = jh.nbytes_used;

    uint32_t targets[] = {INODE_BMAP_IDX, DATA_BMAP_IDX, DATA_START_IDX, iblk_no};
    void *bufs[] = {ibmap, dbmap, root_data, ino_blk};

    for(int i = 0; i < 4; i++) {
        struct data_record dr;
        dr.hdr.type = REC_DATA;
        dr.hdr.size = sizeof(dr.hdr) + sizeof(dr.block_no) + BLOCK_SIZE;
        dr.block_no = targets[i];
        memcpy(dr.data, bufs[i], BLOCK_SIZE);
        journal_write(fd, off, &dr, dr.hdr.size);
        off += dr.hdr.size;
    }

    struct commit_record cr = { .hdr = { .type = REC_COMMIT, .size = sizeof(cr) } };
    journal_write(fd, off, &cr, cr.hdr.size);
    off += cr.hdr.size;

    jh.nbytes_used = off;
    journal_write(fd, 0, &jh, sizeof(jh));
    fsync(fd);
    close(fd);
    printf("Logged creation of \"%s\" to journal.\n", file_name);
}

static void install_journal(int max_commits) {
    int fd = open(IMG, O_RDWR);
    if(fd < 0) die("open");
    struct journal_header jh;
    journal_read(fd, 0, &jh, sizeof(jh));
    if(jh.magic != JOURNAL_MAGIC || jh.nbytes_used <= sizeof(jh)) { close(fd); return; }

    uint32_t off = sizeof(jh), committed = 0;
    while(off < jh.nbytes_used && (max_commits < 0 || (int)committed < max_commits)) {
        uint32_t tx_start = off, tx_off = off;
        int has_commit = 0;
        while(tx_off < jh.nbytes_used) {
            struct rec_header rh;
            journal_read(fd, tx_off, &rh, sizeof(rh));
            tx_off += rh.size;
            if(rh.type == REC_COMMIT) { has_commit = 1; break; }
        }
        if(!has_commit) break;
        uint32_t apply_off = tx_start;
        while(apply_off < tx_off) {
            struct rec_header rh;
            journal_read(fd, apply_off, &rh, sizeof(rh));
            if(rh.type == REC_DATA) {
                struct data_record dr;
                journal_read(fd, apply_off, &dr, rh.size);
                write_block(fd, dr.block_no, dr.data);
            }
            apply_off += rh.size;
        }
        committed++;
        off = tx_off;
    }
    jh.nbytes_used = sizeof(jh);
    journal_write(fd, 0, &jh, sizeof(jh));
    fsync(fd);
    close(fd);
    if(committed) printf("Installed %d committed transactions from journal\n", committed);
}

int main(int argc, char *argv[]) {
    printf("\n");
    if (argc == 3 && !strcmp(argv[1], "create")) {
        create_journal(argv[2]);
        printf("\n"); return 0;
    }
    else if (argc == 2 && !strcmp(argv[1], "install")) {
        install_journal(-1);
        printf("\n"); return 0;
    }
    else {
        printf("Usage:\n  journal create [name]\n  journal install\n\n");
        return 1;
    }
}