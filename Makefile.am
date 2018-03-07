ACLOCAL_AMFLAGS = -I m4

SUBDIRS = src/ffi/include .
EXTRA_DIST = .gitignore autogen.sh

ZBOX_LIB = $(PACKAGE_NAME)
ZBOX_VER_LIB = $(PACKAGE_NAME)-$(PACKAGE_VERSION)
ZBOX_SRC_LIB = target/release/$(ZBOX_LIB)
ZBOX_TGT_LIB = $(DESTDIR)$(libdir)/$(ZBOX_VER_LIB)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = @PACKAGE_NAME@.pc

DISTCLEANFILES = $(pkgconfig_DATA)

rust:
	cargo build --release

clear-lib:
	rm -f $(DESTDIR)$(libdir)/$(ZBOX_LIB)*

all-local: rust

install-exec-local: clear-lib
	$(MKDIR_P) $(DESTDIR)$(libdir) && \
	case "$(host_os)" in \
	  darwin*) \
	    $(install_sh) $(ZBOX_SRC_LIB).a $(ZBOX_TGT_LIB).a; \
	    $(install_sh) $(ZBOX_SRC_LIB).dylib $(ZBOX_TGT_LIB).dylib; \
	    cd $(DESTDIR)$(libdir) && \
	    $(LN_S) $(ZBOX_VER_LIB).a $(ZBOX_LIB).a && \
	    $(LN_S) $(ZBOX_VER_LIB).dylib $(ZBOX_LIB).dylib;; \
	  linux*) \
	    $(install_sh) $(ZBOX_SRC_LIB).a $(ZBOX_TGT_LIB).a; \
	    $(install_sh) $(ZBOX_SRC_LIB).so $(ZBOX_TGT_LIB).so; \
	    cd $(DESTDIR)$(libdir) && \
	    $(LN_S) $(ZBOX_VER_LIB).a $(ZBOX_LIB).a && \
	    $(LN_S) $(ZBOX_VER_LIB).so $(ZBOX_LIB).so;; \
	  *) \
	    echo "Your platform $host_os is not currently supported"; \
	    exit 1;; \
	esac;

clean-local:
	cargo clean

uninstall-local: clear-lib
	rm -rf $(DESTDIR)$(includedir)/zbox
