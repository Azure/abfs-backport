#!/bin/bash

# Script to patch legacy Hadoop with new ABFS driver (hadoop-azure.*.jar) that has been specifically backported
# for the targeted Hadoop distro and version.
# To fully patch a cluster, this script must be run on EVERY node to update the local filesystem. On ONE NODE ONLY
# the -a switch must be specified to patch the HDFS contents.

APPLY_HDFS_PATCH=0
HDFS_USER=
TARGET_RELEASE=
DIR_PREFIX=
HDFS_DIR_PREFIX=

while getopts ":a?hu:t:p:P:" options
do
    case "${options}" in
        a)
            APPLY_HDFS_PATCH=1
            ;;
        u)
            HDFS_USER=${OPTARG}
            ;;
        t)
            TARGET_RELEASE=${OPTARG}
            ;;
        p)
            DIR_PREFIX=${OPTARG}
            ;;
        P)
            HDFS_DIR_PREFIX=${OPTARG}
            ;;
        *|?|h)
            echo "Usage: $0 [-a] [-u HDFS_USER] [-t TARGET_VERSION] [-p DIRECTORY_PREFIX] [-P HDFS_DIRECTORY_PREFIX] [-?]"
            echo ""
            echo "Where:"
            echo "  -a              Update HDFS contents. This switch should only be specified for ONE node in a cluster patch."
            echo "  -u HDFS_USER    Specifies the user name with superuser privileges on HDFS. Applicable only if the -a switch is specified."
            echo "  -t TARGET_VERSION "
            echo "                  Specifies a Release name in the associated Github repo. This release will contain the .jar file to patch."
            echo "  -p DIRECTORY_PREFIX "
            echo "                  Specifies a prefix that is specific to the Hadoop distro & version to search for files to patch."
            echo "  -P HDFS_DIRECTORY_PREFIX "
            echo "                  Specifies a prefix that is specific to the Hadoop distro & version to search on HDFS for files to patch."
            exit 1
            ;;
    esac
done

which jq > /dev/null
if [ $? -ne 0 ]; then

    echo "This script requires jq to run. Please install using preferred package manager"
    exit 1
fi

GITHUB_API_ROOT_URI=https://api.github.com/repos/jamesbak/abfs_backport
# If the TARGET_RELEASE hasn't been explicitly specified, try to determine this from github
if [[ -z "$TARGET_RELEASE" ]]; then

    # The value between the dollar-colon tokens is automatically substituted when committing to git.
    # Do not modify this value or the tokens
    SCRIPT_COMMIT=$(echo "$:d587d4f78979ed63e0e657dc131e486699438052:$" | cut -d '$' -f 2 | cut -d ':' -f 2)

    echo ""
    echo "Determining release associated with script: $SCRIPT_COMMIT"
    # Create a map between tags & associated commits. We have to do some funky imperative logic here because a
    # reference to a tag can return a commit or a tag (which needs to be dereferenced)
    TAGS=$(for tag in $(curl "$GITHUB_API_ROOT_URI/releases" | jq -r ".[].tag_name")
    do

        ref=$(curl "$GITHUB_API_ROOT_URI/git/matching-refs/tags/$tag" | jq -r '.[0].object')
        commit=$(echo $ref | jq -r '.sha')
        # If this is a tag reference, we need to dereference to the commit object
        if [ "tag" == $(echo $ref | jq -r '.type') ]; then

            commit=$(curl $(echo $ref | jq -r '.url') | jq -r '.object.sha')
        fi
        echo '{"commit": "'$commit'", "tag": "'$tag'"}'
    done)

    # Walk through the commits, looking for an associated tag as we walk down until we find our current commit & that is the effective tag
    CURRENT_TAG=
    for commit in $(curl $GITHUB_API_ROOT_URI/commits | jq -r '.[].sha')
    do

        # The embedded commit hash is always for the previous commit, so jump out prior to the current comparison
        if [ "$SCRIPT_COMMIT" == "$commit" ]; then

            TARGET_RELEASE=$CURRENT_TAG
            break
        fi

        # Search in our tags list to see if this commit is associated with a tag - that will become our CURRENT_TAG as we walk down
        COMMIT_TAG=$(echo $TAGS | jq -r '. | select(.commit == "'$commit'") | .tag')
        
        if [ -n "$COMMIT_TAG" ]; then

            CURRENT_TAG=$COMMIT_TAG
        fi
    done
    echo "Using target release: $TARGET_RELEASE"
fi
if [[ -z "$TARGET_RELEASE" ]]; then

    echo "Unable to determine target Hadoop release."
    exit 2
fi

export MATCHED_JAR_FILE_NAME=hadoop-azure
RELEASE_INFO=$(curl "${GITHUB_API_ROOT_URI}/releases/tags/${TARGET_RELEASE}")
JAR_ASSET=$(echo $RELEASE_INFO | jq -r '.assets[] | select(.content_type == "application/java-archive") | .')
if [[ -z "$JAR_ASSET" ]]; then

    echo "Unable to get information for .jar file associated with $TARGET_RELEASE release."
    exit 4
fi
PATCHED_JAR_FILE_NAME=$(basename $(echo $JAR_ASSET | jq -r '.name') .jar)
REMOTE_PATCH_PATH=$(echo $JAR_ASSET | jq -r '.browser_download_url')
LOCAL_PATCH_PATH="/tmp/$PATCHED_JAR_FILE_NAME.new"

# Check for a default properties file for this release. This provides sensible defaults, but can be overridden by commandline args
PROPS_FILE_URL=$(echo $RELEASE_INFO | jq -r '.assets[] | select(.content_type == "application/json") | .browser_download_url')
if [[ -n "$PROPS_FILE_URL" ]]; then

    echo ""
    echo "Found properties file: $PROPS_FILE_URL. Downloading & applying."
    # We only support a whitelisted set of variables
    ALLOWED_VARS=("HDFS_USER" "DIR_PREFIX" "HDFS_DIR_PREFIX")
    for PROP in $(curl -L "$PROPS_FILE_URL" | jq -r '.properties | to_entries | map("\(.key)=\(.value|tostring)") | .[]')
    do

        IFS='=' read -a PROPVALUE <<< $PROP
        # Only overwrite if the variable is in our whitelist & is unassigned
        if [[ " ${ALLOWED_VARS[@]} " =~ " ${PROPVALUE[0]} " && -z ${!PROPVALUE[0]} ]]; then

            read "${PROPVALUE[0]}" <<< "${PROPVALUE[1]}"
        fi
    done
fi
[[ "${DIR_PREFIX}" != */ ]] && DIR_PREFIX="${DIR_PREFIX}/"
[[ "${HDFS_DIR_PREFIX}" != */ ]] && HDFS_DIR_PREFIX="${HDFS_DIR_PREFIX}/"

if [ -e $LOCAL_PATCH_PATH ]; then 

    rm $LOCAL_PATCH_PATH; 
fi
echo ""
echo "Downloading $REMOTE_PATCH_PATH to $LOCAL_PATCH_PATH"
wget $REMOTE_PATCH_PATH -O $LOCAL_PATCH_PATH
if [ $? -ne 0 ]; then

    echo "ERROR: failed to download $REMOTE_PATCH_PATH to $LOCAL_PATCH_PATH"
    exit 3
fi

echo ""
echo "Locating all JAR files in $DIR_PREFIX*.tar.gz"
GZs=$(find "$DIR_PREFIX" -name "*.tar.gz" -print0 | xargs -0 zgrep "$MATCHED_JAR_FILE_NAME" | tr ":" "\n" | grep .tar.gz)
for GZ in $GZs
do

    if [[ ! -e "${GZ}.original" ]]; then

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

echo ""
echo "Updating all JAR files with the same name in $DIR_PREFIX$MATCHED_JAR_FILE_NAME*.jar"
for DST in $(find "$DIR_PREFIX" -name "$MATCHED_JAR_FILE_NAME*.jar" -a ! -name "*datalake*")
do
    echo $DST
    # Different handling for symlink or real file
    if [[ ! -h "$DST" ]]; then

        # Backup original JAR if not already backed up
        if [[ ! -e "${DST}.original" ]]; then

            cp "$DST" "${DST}.original"
        fi

        # Replace with patched JAR
        rm -f "$DST"
        DST="$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
        echo "cp $LOCAL_PATCH_PATH $DST"
        cp "$LOCAL_PATCH_PATH" "$DST"
    else

        # For symlink, assume the target will be replaced with the correctly named file. Just update the link.
        NEW_TARGET="$(dirname $(readlink "$DST"))/$PATCHED_JAR_FILE_NAME.jar"
        ln -sfn "$NEW_TARGET" "$DST"
    fi
done

# HDFS update
if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo ""
    echo "Updating all JAR files on HDFS matching; $HDFS_DIR_PREFIX$MATCHED_JAR_FILE_NAME*.jar"
    for HDST in $(sudo -u $HDFS_USER hadoop fs -find "$HDFS_DIR_PREFIX" -name "$MATCHED_JAR_FILE_NAME*.jar" | grep -v "datalake")
    do

        sudo -u $HDFS_USER hadoop fs -test -e "${HDST}.original"
        if [ $? -ne 0 ]; then

            sudo -u $HDFS_USER hadoop fs -cp "$HDST" "${HDST}.original"
        fi

        sudo -u $HDFS_USER hadoop fs -rm $HDST
        HDST="$(dirname "$HDST")/$PATCHED_JAR_FILE_NAME.jar"
        echo "hadoop fs -put -f $LOCAL_PATCH_PATH $HDST"
        sudo -u $HDFS_USER hadoop fs -put -f "$LOCAL_PATCH_PATH" "$HDST"
    done
fi

echo ""
echo "Updating all .tar.gz"
for GZ in $GZs
do

    echo "tar -czf $GZ -C ${GZ}.dir"
    tar -czf "$GZ" -C "${GZ}.dir" .
    rm -rf "${GZ}.dir"
done

if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo ""
    echo "Updating all .tar.gz files on HDFS matching $HDFS_DIR_PREFIX*.tar.gz"
    for HGZ in $(sudo -E -u $HDFS_USER hadoop fs -find "$HDFS_DIR_PREFIX" -name "*.tar.gz" -print0 | xargs -0 -I % sudo -E sh -c 'hadoop fs -cat % | tar -tzv | grep "$MATCHED_JAR_FILE_NAME" && echo %' | grep ".tar.gz")
    do

        echo "$HGZ"
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
            cp "$LOCAL_PATCH_PATH" "$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
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
