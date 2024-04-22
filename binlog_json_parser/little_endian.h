#ifndef LITTLE_ENDIAN_INCLUDED
#define LITTLE_ENDIAN_INCLUDED
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
@file include/little_endian.h
Data in little-endian format.
*/

// IWYU pragma: private, include "my_byteorder.h"

#include <string.h>

#include <stdint.h>

/*
Since the pointers may be misaligned, we cannot do a straight read out of
them. (It usually works-by-accident on x86 and on modern ARM, but not always
when the compiler chooses unusual instruction for the read, e.g. LDM on ARM
or most SIMD instructions on x86.) memcpy is safe and gets optimized to a
single operation, since the size is small and constant.
*/

static inline int16_t sint2korr(const unsigned char *A) {
  int16_t ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline int32_t sint4korr(const unsigned char *A) {
  int32_t ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline uint16_t uint2korr(const unsigned char *A) {
  uint16_t ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline uint32_t uint4korr(const unsigned char *A) {
  uint32_t ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline unsigned long long uint8korr(const unsigned char *A) {
  unsigned long long ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline long long sint8korr(const unsigned char *A) {
  long long ret;
  memcpy(&ret, A, sizeof(ret));
  return ret;
}

static inline void int2store(unsigned char *T, uint16_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int4store(unsigned char *T, uint32_t A) { memcpy(T, &A, sizeof(A)); }

static inline void int7store(unsigned char *T, unsigned long long A) { memcpy(T, &A, 7); }

static inline void int8store(unsigned char *T, unsigned long long A) {
  memcpy(T, &A, sizeof(A));
}

static inline float float4get(const unsigned char *M) {
  float V;
  memcpy(&V, (M), sizeof(float));
  return V;
}

static inline void float4store(unsigned char *V, float M) {
  memcpy(V, (&M), sizeof(float));
}

static inline double float8get(const unsigned char *M) {
  double V;
  memcpy(&V, M, sizeof(double));
  return V;
}

static inline void float8store(unsigned char *V, double M) {
  memcpy(V, &M, sizeof(double));
}

#endif /* LITTLE_ENDIAN_INCLUDED */
