#include <R.h>
#include <Rdefines.h>
#include <stdio.h>

SEXP C_validate_key(SEXP key_r) {
  if (TYPEOF(key_r) != STRSXP || Rf_length(key_r) != 1) {
    Rf_error("key must be a one-element character vector");
  }
  SEXP key_c = STRING_ELT(key_r, 0);
  if (key_c == NA_STRING || Rf_StringBlank(key_c)) {
    Rf_error("key must be not be \"\" or NA");
  }

  const char* s = R_CHAR(key_c);
  char c = *s;
  while (c != 0) {
    int valid = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z');
    if (!valid) {
      Rf_error("Invalid key: %s. Only lowercase letters and numbers are allowed.", s);
    }
    s++;
    c = *s;
  }

  return Rf_ScalarLogical(TRUE);
}

