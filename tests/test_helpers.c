#include "test_helpers.h"

#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

void rm_recursive(const char *path)
{
    DIR *d = opendir(path);
    if (!d)
        return;

    struct dirent *entry;
    char filepath[1024];

    while ((entry = readdir(d)) != NULL) {
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
            continue;

        snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);
        struct stat st;
        if (stat(filepath, &st) == 0 && S_ISDIR(st.st_mode))
            rm_recursive(filepath);
        else
            unlink(filepath);
    }

    closedir(d);
    rmdir(path);
}
