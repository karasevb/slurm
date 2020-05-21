/*****************************************************************************\
 *  base64_encode.c - Base64 binary-to-text encoding
 *****************************************************************************
 *  Copyright (C) 2020      Mellanox Technologies. All rights reserved.
 *  Written by Boris Karasev <karasev.b@gmail.com, boriska@mellanox.com>.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "src/common/xassert.h"
#include "src/common/xmalloc.h"

/* _base64_enc_tbl has 64 elements and allows to encode 6 bit of information,
 * thus to encode integral 8-bit bytes, we need to encode 3 bytes (3*8=24 bits)
 * that will be represented as 4 elements (4*6=24 bits)  from the table.
 * This is a minimal chunk of information encoded with base64 (word).
 * Encoded word occupies 4B in the buffer, decoded word - 3B. */

#define _ENC_WORD 4
#define _DEC_WORD 3

#define _ENC_LEN(input_length) (_ENC_WORD * (((input_length) + 2)/_DEC_WORD))

#define _DEC_LEN(input_length, padding_legth) \
	(_DEC_WORD * (input_length)/_ENC_WORD - (padding_legth))

#define _ENC_WORD_NUM(input_length, padding_legth) \
	((((input_length) - (padding_legth)) / _ENC_WORD) * _ENC_WORD)

#define _DEC_WORD_NUM(input_length) (((input_length)/_DEC_WORD)*_DEC_WORD)

static char _enc_tbl[] = {
	'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
	'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
	'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
	'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f',
	'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
	'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
	'w', 'x', 'y', 'z', '0', '1', '2', '3',
	'4', '5', '6', '7', '8', '9', '+', '/',
};

static char _dec_tbl[256];

static void _decode_init()
{
	int i;
	for (i = 0; i < 64; i++) {
		_dec_tbl[(int)(_enc_tbl[i])] = i;
	}
}

char *slurm_base64_encode(char *buf, size_t length, size_t *enc_len)
{
	char *enc_str;
	size_t i, j = 0;

	if (!length) {
		return NULL;
	}
	xassert(buf);

	*enc_len = _ENC_LEN(length);
	enc_str = (char*)xmalloc(*enc_len + 1);
	if (!enc_str) {
		return NULL;
	}

	enc_str[*enc_len] = '\0';

	for (i = 0; i < _DEC_WORD_NUM(length); i += _DEC_WORD) {
		enc_str[j++] = _enc_tbl[(buf[i] >> 2) & 0x3f];
		enc_str[j++] = _enc_tbl[((buf[i] & 0x03) << 4) |
				((buf[i+1] & 0xF0) >> 4)];
		enc_str[j++] = _enc_tbl[(buf[i+1] & 0x0F) << 2 |
				((buf[i+2] & 0xC0) >> 6)];
		enc_str[j++] = _enc_tbl[(buf[i+2] & 0x3F)];
	}
	if ((length - i) == 1) {
		enc_str[j++] = _enc_tbl[(buf[i] >> 2) & 0x3f];
		enc_str[j++] = _enc_tbl[(buf[i] & 0x03) << 4];
		enc_str[j++] = '=';
		enc_str[j++] = '=';
	} else if ((length - i) == 2) {
		enc_str[j++] = _enc_tbl[(buf[i] >> 2) & 0x3f];
		enc_str[j++] = _enc_tbl[((buf[i] & 0x03) << 4) |
				((buf[i+1] & 0xF0) >> 4)];
		enc_str[j++] = _enc_tbl[(buf[i+1] & 0x0F) << 2];
		enc_str[j++] = '=';
	}
	xassert(*enc_len == j);

	return enc_str;
}

char *slurm_base64_decode(char *buf, size_t length, size_t *dec_len)
{
	char *dec_str;
	int i, j = 0;
	size_t pad_len = 0;

	if (!length) {
		return NULL;
	}
	xassert(buf);

	_decode_init();

	if (buf[length-1] == '=') {
		pad_len++;
	}
	if (buf[length-2] == '=') {
		pad_len++;
	}

	*dec_len = _DEC_LEN(length, pad_len);
	dec_str = (char*)xmalloc(*dec_len);
	if (!dec_str) {
		return NULL;
	}

	i = 0;
	for (i = 0; i < _ENC_WORD_NUM(length, pad_len); i += _ENC_WORD) {
		dec_str[j++] = (_dec_tbl[(int)buf[i]] << 2) |
				(_dec_tbl[(int)buf[i + 1]] >> 4);
		dec_str[j++] = (_dec_tbl[(int)buf[i+1]] << 4) |
				(_dec_tbl[(int)buf[i + 2]] >> 2);
		dec_str[j++] = (_dec_tbl[(int)buf[i+2]] << 6) |
				_dec_tbl[(int)buf[i + 3]];
	}
	if (pad_len) {
		dec_str[j++] = (_dec_tbl[(int)buf[i]] << 2) |
				(_dec_tbl[(int)buf[i + 1]] >> 4);
	}
	if (pad_len == 1) {
		dec_str[j++] = (_dec_tbl[(int)buf[i+1]] << 4) |
				(_dec_tbl[(int)buf[i + 2]] >> 2);
	}
	xassert(*dec_len == j);

	return dec_str;
}
