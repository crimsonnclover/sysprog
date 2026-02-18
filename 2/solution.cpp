#include "parser.h"

#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>

#include <string>
#include <vector>


void
cleanup_zombies() {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {}
}


static int
execute_pipeline (
    const std::vector<const expr*>& exprs, 
    bool is_background, 
    const std::string& out_file, 
    int out_type, 
    int current_status
) {
    if (exprs.empty()) return current_status;

    int prev_read_fd = STDIN_FILENO;
    std::vector<pid_t> pids;

    for (size_t i = 0; i < exprs.size(); ++i) {
        if (exprs[i]->type != EXPR_TYPE_COMMAND) continue;

        const command& cmd = exprs[i]->cmd.value();
        bool has_next_pipe = (i + 1 < exprs.size() && exprs[i + 1]->type == EXPR_TYPE_PIPE);

        if (exprs.size() == 1) {
            if (cmd.exe == "exit") {
                int code = cmd.args.empty() ? current_status : std::stoi(cmd.args[0]);
                _exit(code);
            }
            if (cmd.exe == "cd") {
                if (cmd.args.empty()) return 0;
                const char* path = cmd.args[0].c_str();
                if (chdir(path) != 0) {
                    perror("cd");
                    return 1;
                }
                return 0;
            }
        }

        int pipe_fds[2] = {-1, -1};
        if (has_next_pipe) {
            if (pipe(pipe_fds) == -1) { 
                perror("pipe"); 
                break; 
            }
        }

        pid_t pid = fork();
        if (pid == 0) {
            if (prev_read_fd != STDIN_FILENO) {
                dup2(prev_read_fd, STDIN_FILENO);
                close(prev_read_fd);
            }

            if (has_next_pipe) {
                close(pipe_fds[0]);
                dup2(pipe_fds[1], STDOUT_FILENO);
                close(pipe_fds[1]);
            } 
            else if (out_type != OUTPUT_TYPE_STDOUT) {
                int flags = O_WRONLY | O_CREAT | (out_type == OUTPUT_TYPE_FILE_NEW ? O_TRUNC : O_APPEND);
                int out_fd = open(out_file.c_str(), flags, 0644);
                dup2(out_fd, STDOUT_FILENO);
                close(out_fd);
            }

            if (cmd.exe == "exit") {
                int code = cmd.args.empty() ? current_status : std::stoi(cmd.args[0]);
                _exit(code);
            }
            if (cmd.exe == "cd") _exit(0);

            std::vector<char*> c_args;
            c_args.push_back(const_cast<char*>(cmd.exe.c_str()));
            for (const auto& s : cmd.args) c_args.push_back(const_cast<char*>(s.c_str()));
            c_args.push_back(nullptr);

            execvp(c_args[0], c_args.data());
            
            perror("execvp");
            _exit(1);
        }

        if (pid != 0) {
            pids.push_back(pid);
        }

        if (prev_read_fd != STDIN_FILENO) close(prev_read_fd);
        if (has_next_pipe) {
            prev_read_fd = pipe_fds[0];
            close(pipe_fds[1]);
        }
    }

    int pipeline_status = current_status;
    
    if (!is_background) {
        for (pid_t p : pids) {
            int status;
            waitpid(p, &status, 0);

            if (WIFEXITED(status)) pipeline_status = WEXITSTATUS(status);
            else if (WIFSIGNALED(status)) pipeline_status = 128 + WTERMSIG(status);
        }
    } 
    else {
        /* Для процесса в фоне статус 0 */
        pipeline_status = 0;
    }
    
    return pipeline_status;
}


static int
execute_command_line(const struct command_line *line, int current_status) {
    if (line->exprs.empty()) return current_status;

    /* Ленивая очистка зомби процессов при каждой новой команде */
    cleanup_zombies();

    std::vector<const expr*> current_pipeline;
    int pipeline_status = current_status;
    bool skip_next_execution = false;

    /* Предполагается, что перенаправление вывода с логическими 
    операндами не реализуется (даже парсер этого не реализует) */
    for (const auto& e : line->exprs) {
        if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {

            if (!current_pipeline.empty()) {
                if (!skip_next_execution) {
                    pipeline_status = execute_pipeline(
                        current_pipeline,
                        line->is_background,
                        line->out_file,
                        line->out_type,
                        pipeline_status
                    );
                }
                current_pipeline.clear();
            }

            if (e.type == EXPR_TYPE_AND) skip_next_execution = (pipeline_status != 0);
            if (e.type == EXPR_TYPE_OR) skip_next_execution = (pipeline_status == 0);
        }
        else {
            current_pipeline.push_back(&e);
        }
    }

    if (!current_pipeline.empty()) {
        if (!skip_next_execution) {
            pipeline_status = execute_pipeline(
                current_pipeline,
                line->is_background,
                line->out_file,
                line->out_type,
                pipeline_status);
        }
    }
    return pipeline_status;
}


int
main(void)
{
    const size_t buf_size = 1024;
    char buf[buf_size];
    int rc;
    int last_status = 0;

    struct parser *p = parser_new();
    while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
        parser_feed(p, buf, rc);
        struct command_line *line = NULL;
        while (true) {
            enum parser_error err = parser_pop_next(p, &line);
            if (err == PARSER_ERR_NONE && line == NULL)
                break;
            if (err != PARSER_ERR_NONE) {
                printf("Error: %d\n", (int)err);
                continue;
            }
            
            last_status = execute_command_line(line, last_status);
            delete line;
        }
    }
    parser_delete(p);
    
    return last_status;
}
