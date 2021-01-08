#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

source $(dirname $0)/pack_common.sh

function usage()
{
    echo "Options for subcommand 'pack_tools':"
    echo "  -h"
    echo "  -p|--update-package-template <minos-package-template-file-path>"
    echo "  -g|--custom-gcc"
    exit 0
}

pwd="$( cd "$( dirname "$0"  )" && pwd )"
shell_dir="$( cd $pwd/.. && pwd )"
cd $shell_dir

if [ ! -f src/include/pegasus/git_commit.h ]
then
    echo "ERROR: src/include/pegasus/git_commit.h not found"
    exit 1
fi

if [ ! -f DSN_ROOT/bin/pegasus_shell/pegasus_shell ]
then
    echo "ERROR: DSN_ROOT/bin/pegasus_shell/pegasus_shell not found"
    exit 1
fi

if [ ! -f src/builder/CMAKE_OPTIONS ]
then
    echo "ERROR: src/builder/CMAKE_OPTIONS not found"
    exit 1
fi

if grep -q Debug src/builder/CMAKE_OPTIONS
then
    build_type=debug
else
    build_type=release
fi
version=`grep "VERSION" src/include/pegasus/version.h | cut -d "\"" -f 2`
commit_id=`grep "GIT_COMMIT" src/include/pegasus/git_commit.h | cut -d "\"" -f 2`
glibc_ver=`ldd --version | grep ldd | grep -Eo "[0-9]+.[0-9]+$"`
echo "Packaging pegasus tools $version ($commit_id) glibc-$glibc_ver $build_type ..."

pack_version=tools-$version-${commit_id:0:7}-glibc${glibc_ver}-${build_type}
pack=pegasus-$pack_version

if [ -f ${pack}.tar.gz ]
then
    rm -f ${pack}.tar.gz
fi

if [ -d ${pack} ]
then
    rm -rf ${pack}
fi

pack_template=""
if [ -n "$MINOS_CONFIG_FILE" ]; then
    pack_template=`dirname $MINOS_CONFIG_FILE`/xiaomi-config/package/pegasus.yaml
fi

custom_gcc="false"

while [[ $# > 0 ]]; do
    option_key="$1"
    case $option_key in
        -p|--update-package-template)
            pack_template="$2"
            shift
            ;;
        -g|--custom-gcc)
            custom_gcc="true"
            ;;
        -h|--help)
            usage
            ;;
    esac
    shift
done

mkdir -p ${pack}
copy_file ./run.sh ${pack}/

mkdir -p ${pack}/DSN_ROOT/bin
cp -v -r ./DSN_ROOT/bin/pegasus_server ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_shell ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_bench ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_kill_test ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_rproxy ${pack}/DSN_ROOT/bin/
cp -v -r ./DSN_ROOT/bin/pegasus_pressureclient ${pack}/DSN_ROOT/bin/

mkdir -p ${pack}/DSN_ROOT/lib
copy_file ./DSN_ROOT/lib/*.so* ${pack}/DSN_ROOT/lib/
copy_file ./rdsn/thirdparty/output/lib/libPoco*.so.48 ${pack}/DSN_ROOT/lib/
copy_file ./rdsn/thirdparty/output/lib/libtcmalloc_and_profiler.so.4 ${pack}/DSN_ROOT/lib/
copy_file ./rdsn/thirdparty/output/lib/libboost*.so.1.69.0 ${pack}/DSN_ROOT/lib/
copy_file `get_stdcpp_lib $custom_gcc` ${pack}/DSN_ROOT/lib/

pack_system_lib() {
    SYS_LIB_PATH=$(get_system_lib shell "$1")
    if [ -z "${SYS_LIB_PATH}" ]; then
        echo "ERROR: library $1 is missing on your system"
        exit 1
    fi
    SYS_LIB_NAME=$(get_system_libname shell "$1")
    copy_file "${SYS_LIB_PATH}" "${pack}/DSN_ROOT/lib/${SYS_LIB_NAME}"
}

pack_system_lib snappy
pack_system_lib crypto
pack_system_lib ssl
pack_system_lib zstd
pack_system_lib lz4

chmod -x ${pack}/DSN_ROOT/lib/*

mkdir -p ${pack}/scripts
copy_file ./scripts/* ${pack}/scripts/
chmod +x ${pack}/scripts/*.sh

mkdir -p ${pack}/src/server
copy_file ./src/server/*.ini ${pack}/src/server/

mkdir -p ${pack}/src/shell
copy_file ./src/shell/*.ini ${pack}/src/shell/

mkdir -p ${pack}/src/test/kill_test
copy_file ./src/test/kill_test/*.ini ${pack}/src/test/kill_test/

echo "Pegasus Tools $version ($commit_id) $platform $build_type" >${pack}/VERSION

tar cfz ${pack}.tar.gz ${pack}

if [ -f $pack_template ]; then
    echo "Modifying $pack_template ..."
    sed -i "/^version:/c version: \"$pack_version\"" $pack_template
    sed -i "/^build:/c build: \"\.\/run.sh pack_tools\"" $pack_template
    sed -i "/^source:/c source: \"$PEGASUS_ROOT\"" $pack_template
fi

echo "Done"
