/* Copyright (c) 2012, 2023, Oracle and/or its affiliates.

This program is free software; you can redistribute it and/or modify
        it under the terms of the GNU General Public License, version 2.0,
    as published by the Free Software Foundation.

    This program is also distributed with certain software (including
                 but not limited to OpenSSL) that is licensed under separate terms,
    as designated in a particular file or component or in included license
                                                       documentation.  The authors of MySQL hereby grant you an additional
                                                       permission to link the program and your derivative works with the
                                                       separately licensed software that they have included with MySQL.

                                                       This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
          Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
@file include/big_endian.h

Endianness-independent definitions (little_endian.h contains optimized
versions if you know you are on a little-endian platform).
*/

// IWYU pragma: private, include "my_byteorder.h"


#include <string.h>

#include <stdint.h>


static inline int16_t sint2korr(const unsigned char *A) {
  return (int16_t)(((int16_t)(A[0])) + ((int16_t)(A[1]) << 8));
}

static inline int32_t sint4korr(const unsigned char *A) {
  return (int32_t)(((int32_t)(A[0])) + (((int32_t)(A[1]) << 8)) +
                   (((int32_t)(A[2]) << 16)) + (((int32_t)(A[3]) << 24)));
}

static inline uint16_t uint2korr(const unsigned char *A) {
  return (uint16_t)(((uint16_t)(A[0])) + ((uint16_t)(A[1]) << 8));
}

static inline uint32_t uint4korr(const unsigned char *A) {
  return (uint32_t)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
                    (((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24));
}

static inline unsigned long long uint8korr(const unsigned char *A) {
  return ((unsigned long long)(((uint32_t)(A[0])) + (((uint32_t)(A[1])) << 8) +
                               (((uint32_t)(A[2])) << 16) + (((uint32_t)(A[3])) << 24)) +
          (((unsigned long long)(((uint32_t)(A[4])) + (((uint32_t)(A[5])) << 8) +
                                 (((uint32_t)(A[6])) << 16) + (((uint32_t)(A[7])) << 24)))
              << 32));
}

static inline long long sint8korr(const unsigned char *A) {
  return (long long)uint8korr(A);
}

static inline void int2store(unsigned char *T, uint16_t A) {
  const unsigned int def_temp = A;
  *(T) = (unsigned char)(def_temp);
  *(T + 1) = (unsigned char)(def_temp >> 8);
}

static inline void int4store(unsigned char *T, uint32_t A) {
  *(T) = (unsigned char)(A);
  *(T + 1) = (unsigned char)(A >> 8);
  *(T + 2) = (unsigned char)(A >> 16);
  *(T + 3) = (unsigned char)(A >> 24);
}

static inline void int7store(unsigned char *T, unsigned long long A) {
  *(T) = (unsigned char)(A);
  *(T + 1) = (unsigned char)(A >> 8);
  *(T + 2) = (unsigned char)(A >> 16);
  *(T + 3) = (unsigned char)(A >> 24);
  *(T + 4) = (unsigned char)(A >> 32);
  *(T + 5) = (unsigned char)(A >> 40);
  *(T + 6) = (unsigned char)(A >> 48);
}

static inline void int8store(unsigned char *T, unsigned long long A) {
  const unsigned int def_temp = (unsigned int)A, def_temp2 = (unsigned int)(A >> 32);
  int4store(T, def_temp);
  int4store(T + 4, def_temp2);
}

/*
  Data in big-endian format.
*/
static inline void float4store(unsigned char *T, float A) {
  *(T) = ((unsigned char *)&A)[3];
  *((T) + 1) = (char)((unsigned char *)&A)[2];
  *((T) + 2) = (char)((unsigned char *)&A)[1];
  *((T) + 3) = (char)((unsigned char *)&A)[0];
}

static inline float float4get(const unsigned char *M) {
  float def_temp = 0;
  ((unsigned char *)&def_temp)[0] = (M)[3];
  ((unsigned char *)&def_temp)[1] = (M)[2];
  ((unsigned char *)&def_temp)[2] = (M)[1];
  ((unsigned char *)&def_temp)[3] = (M)[0];
  return def_temp;
}

static inline void float8store(unsigned char *T, double V) {
  *(T) = ((unsigned char *)&V)[7];
  *((T) + 1) = (char)((unsigned char *)&V)[6];
  *((T) + 2) = (char)((unsigned char *)&V)[5];
  *((T) + 3) = (char)((unsigned char *)&V)[4];
  *((T) + 4) = (char)((unsigned char *)&V)[3];
  *((T) + 5) = (char)((unsigned char *)&V)[2];
  *((T) + 6) = (char)((unsigned char *)&V)[1];
  *((T) + 7) = (char)((unsigned char *)&V)[0];
}

static inline double float8get(const unsigned char *M) {
  double def_temp = 0;
  ((unsigned char *)&def_temp)[0] = (M)[7];
  ((unsigned char *)&def_temp)[1] = (M)[6];
  ((unsigned char *)&def_temp)[2] = (M)[5];
  ((unsigned char *)&def_temp)[3] = (M)[4];
  ((unsigned char *)&def_temp)[4] = (M)[3];
  ((unsigned char *)&def_temp)[5] = (M)[2];
  ((unsigned char *)&def_temp)[6] = (M)[1];
  ((unsigned char *)&def_temp)[7] = (M)[0];
  return def_temp;
}
