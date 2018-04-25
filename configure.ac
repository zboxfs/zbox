dnl                                               -*- Autoconf -*-
dnl Process this file with autoconf to produce a configure script.

dnl Semantic Versioning, http://semver.org
m4_define([zbox_version_major], [0])
m4_define([zbox_version_minor], [5])
m4_define([zbox_version_patch], [1])

AC_PREREQ([2.69])
AC_INIT([libzbox],
  [zbox_version_major.zbox_version_minor.zbox_version_patch],
  [https://github.com/zboxfs/zbox/issues],
  [libzbox],
  [https://github.com/zboxfs/zbox])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIRS([m4])
AC_CONFIG_SRCDIR([src/ffi/include/zbox.h])
AC_CONFIG_HEADERS([config.h])
AM_INIT_AUTOMAKE([foreign subdir-objects -Wall -Werror])

dnl ====================
dnl libtool init
dnl ====================
dnl turn on warning flags
CFLAGS="$CFLAGS -Wall -Werror -Wno-unused-function"

LT_INIT()

dnl ====================
dnl Checks for programs
dnl ====================
AC_PROG_CXX
AC_PROG_AWK
AC_PROG_CC_C99
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S

dnl =========================
dnl Checks for header files.
dnl =========================
AC_CHECK_HEADERS([fcntl.h inttypes.h limits.h stddef.h stdint.h stdlib.h string.h unistd.h getopt.h])

dnl ================================
dnl Checks for typedefs, structures,
dnl and compiler characteristics.
dnl ================================
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT8_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT8_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T
AC_CHECK_TYPES([ptrdiff_t])

dnl Define version variables
AC_SUBST(ZBOX_VERSION_MAJOR, zbox_version_major)
AC_SUBST(ZBOX_VERSION_MINOR, zbox_version_minor)
AC_SUBST(ZBOX_VERSION_PATCH, zbox_version_patch)

dnl ==============================
dnl Generate Makefiles and output
dnl ==============================
dnl Makefiles need to be generated
AC_CONFIG_FILES([ \
  Makefile \
  libzbox.pc \
  src/ffi/include/Makefile \
  src/ffi/include/zbox/version.h \
])

AC_OUTPUT

AC_MSG_RESULT([
  $PACKAGE_NAME $VERSION

    CC:              ${CC}
    CFLAGS:          ${CFLAGS}
    CXX:             ${CXX}
    CXXFLAGS:        ${CXXFLAGS}
    LDFLAGS:         ${LDFLAGS}
    LIBS:            ${LIBS}

    prefix:          ${prefix}
    sysconfdir:      ${sysconfdir}
    libdir:          ${libdir}
    includedir:      ${includedir}
    pkgconfigdir:    ${pkgconfigdir}
])
