#ifndef FOO_H
#define FOO_H

#ifdef __cplusplus
extern "C" {
#endif

#define BUFLEN 4096

extern char message[BUFLEN];

int initialize(int argc, char ** argv);
int run(void);
int terminate(void);

#ifdef __cplusplus
}
#endif

#endif /* FOO_H*/
