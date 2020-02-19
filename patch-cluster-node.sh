#!/bin/bash
APPLY_HDFS_PATCH=${1:-0}
HDFS_USER=${2:-hdfs}
# Revise this value for each release
TARGET_RELEASE=${3:-HDP-2.6.1}

which jq > /dev/null
if [ $? -ne 0 ]; then

    echo "This script requires jq to run. Please install using preferred package manager"
    exit 1
fi

$Id$ 

export MATCHED_JAR_FILE_NAME=hadoop-azure
GITHUB_API_ROOT_URI=https://api.github.com/repos/jamesbak/abfs_backport
PATCHED_JAR_FILE_NAME=$(basename $(curl "${GITHUB_API_ROOT_URI}/releases/tags/${TARGET_RELEASE}" | jq -r '.assets[0].name') .jar)
REMOTE_HOTFIX_PATH=$(curl "${GITHUB_API_ROOT_URI}/releases/tags/${TARGET_RELEASE}" | jq -r '.assets[0]'.browser_download_url)
LOCAL_HOTFIX_PATH="/tmp/$PATCHED_JAR_FILE_NAME.new"

if `test -e $LOCAL_HOTFIX_PATH`; then 

    rm $LOCAL_HOTFIX_PATH; 
fi
echo "Downloading $REMOTE_HOTFIX_PATH to $LOCAL_HOTFIX_PATH"
wget $REMOTE_HOTFIX_PATH -O $LOCAL_HOTFIX_PATH
if [ $? -ne 0 ]; then

    echo "ERROR: failed to download $REMOTE_HOTFIX_PATH to $LOCAL_HOTFIX_PATH"
    exit 1
fi

echo "Locating all JAR files in .tar.gz"
GZs=$(find / -name "*.tar.gz" -print0 | xargs -0 zgrep "$MATCHED_JAR_FILE_NAME" | tr ":" "\n" | grep .tar.gz)
for GZ in $GZs
do

    test -e "${GZ}.original"

    if [ $? -ne 0 ]; then

        cp "$GZ" "${GZ}.original"
    fi

    ARCHIVE_DIR="${GZ}.dir"
    if [[ -d $ARCHIVE_DIR ]]; then

        rm -rf "$ARCHIVE_DIR"
    fi
    mkdir "$ARCHIVE_DIR"
    echo "tar -C "$ARCHIVE_DIR" -zxf $GZ"
    tar -C "$ARCHIVE_DIR" -zxf "$GZ"
done

echo "Updating all JAR files with the same name"
for DST in $(find / -name "$MATCHED_JAR_FILE_NAME*.jar" -a ! -name "*datalake*")
do
    echo $DST
    # only update the file if it is not a symbolic link

    test -h "$DST"
    if [ $? -ne 0 ]; then

        # Backup original JAR if not already backed up
        test -e "${DST}.original"
        if [ $? -ne 0 ]; then

            cp "$DST" "${DST}.original"
        fi

        # Replace with hotfix JAR
        rm -f "$DST"
        DST="$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
        echo "cp $LOCAL_HOTFIX_PATH $DST"
        cp "$LOCAL_HOTFIX_PATH" "$DST"
    fi
done

# only update HDFS files from primary head node
if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo "Updating all JAR files on HDFS"
    for HDST in $(sudo -u $HDFS_USER hadoop fs -find / -name "$MATCHED_JAR_FILE_NAME*.jar" | grep -v "datalake")
    do

        sudo -u $HDFS_USER hadoop fs -test -e "${HDST}.original"
        if [ $? -ne 0 ]; then

            sudo -u $HDFS_USER hadoop fs -cp "$HDST" "${HDST}.original"
        fi

        sudo -u $HDFS_USER hadoop fs -rm $HDST
        HDST="$(dirname "$HDST")/$PATCHED_JAR_FILE_NAME.jar"
        echo "hadoop fs -put -f $LOCAL_HOTFIX_PATH $HDST"
        sudo -u $HDFS_USER hadoop fs -put -f "$LOCAL_HOTFIX_PATH" "$HDST"
    done
fi

echo "Updating all .tar.gz"
for GZ in $GZs
do

    echo "tar -czf $GZ -C ${GZ}.dir"
    tar -czf "$GZ" -C "${GZ}.dir" .
    rm -rf "${GZ}.dir"
done

if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo "Updating all .tar.gz files on HDFS"
    for HGZ in $(sudo -E -u $HDFS_USER hadoop fs -find / -name "*.tar.gz" -print0 | xargs -0 -I % sudo -E sh -c 'hadoop fs -cat % | tar -tzv | grep "$MATCHED_JAR_FILE_NAME" && echo %' | grep ".tar.gz")
    do

        # Create backup
        sudo -u $HDFS_USER hadoop fs -test -e "${HGZ}.original"
        if [ $? -ne 0 ]; then

            sudo -u $HDFS_USER hadoop fs -cp "$HGZ" "${HGZ}.original"
        fi

        # Get the archive, update it with the new jar, repackage the archive & copy it to HDFS
        ARCHIVE_NAME=$(basename $HGZ)
        ARCHIVE_DIR=/tmp/${ARCHIVE_NAME}.dir
        LOCAL_TAR_FILE=/tmp/$ARCHIVE_NAME

        if [[ -e $LOCAL_TAR_FILE ]]; then

            rm -f $LOCAL_TAR_FILE;
        fi
        sudo -u $HDFS_USER hadoop fs -copyToLocal "$HGZ" "$LOCAL_TAR_FILE"

        if [[ -d $ARCHIVE_DIR ]]; then

            rm -rf $ARCHIVE_DIR
        fi
        mkdir $ARCHIVE_DIR
        tar -xzf $LOCAL_TAR_FILE -C $ARCHIVE_DIR

        for DST in $(find $ARCHIVE_DIR -name "$MATCHED_JAR_FILE_NAME*.jar" -a ! -name "*datalake*")
        do

            # Backup original JAR if not already backed up
            if [[ ! -e "${DST}.original" ]]; then

                cp "$DST" "${DST}.original"
            fi
            rm -f "$DST"
            cp "$LOCAL_HOTFIX_PATH" "$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
        done

        cd $ARCHIVE_DIR
        tar -zcf $LOCAL_TAR_FILE *
        cd ..

        echo "hadoop fs -copyFromLocal -p -f $LOCAL_TAR_FILE $HGZ"
        sudo -u $HDFS_USER hadoop fs -copyFromLocal -p -f "$LOCAL_TAR_FILE" "$HGZ"
        rm -rf $ARCHIVE_DIR
        rm -f $LOCAL_TAR_FILE
    done
fi

