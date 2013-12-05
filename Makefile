#
# Copyright (c) 2012, Joyent, Inc. All rights reserved.
#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Tools
#
ISTANBUL	:= ./node_modules/.bin/istanbul
NPM		:= npm
NODEUNIT	:= ./node_modules/.bin/nodeunit

#
# Files
#
DOC_FILES	 = index.restdown
JS_FILES	:= $(shell find lib test -name '*.js')
JSON_FILES	 = package.json
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS	 = -f tools/jsstyle.conf

include ./tools/mk/Makefile.defs

#
# Variables
#
ifeq ($(shell uname -s),Darwin)
	OPEN=/usr/bin/open
else
	OPEN=/usr/bin/false
endif


#
# Repo-specific targets
#
.PHONY: all
all: $(SMF_MANIFESTS) | $(NODEUNIT) $(REPO_DEPS)
	$(NPM) rebuild

$(TAP): | $(NPM_EXEC)
	$(NPM) install

CLEAN_FILES += ./node_modules ./coverage

.PHONY: test
test: $(NODEUNIT)
	$(NPM) test

.PHONY: cover
cover: $(ISTANBUL)
	$(NPM) test --cover
	$(ISTANBUL) report html
	$(OPEN) ./coverage/index.html

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
