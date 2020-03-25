#!/bin/bash

# Script to patch legacy Hadoop with new ABFS driver (hadoop-azure.*.jar) that has been specifically backported
# for the targeted Hadoop distro and version.
# To fully patch a cluster, this script must be run on EVERY node to update the local filesystem. On ONE NODE ONLY
# the -a switch must be specified to patch the HDFS contents.

# Parameters
APPLY_HDFS_PATCH=0
HDFS_USER=
TARGET_RELEASE=
DIR_PREFIX=
HDFS_DIR_PREFIX=
ROLLBACK=0

# Constants
export MATCHED_JAR_FILE_NAME=hadoop-azure
GITHUB_API_ROOT_URI=https://api.github.com/repos/jamesbak/abfs_backport
BACKUP_SUFFIX=".original"
JAR_EXTENSION=".jar"
#
JAR_FIND_SUFFIX=""

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
        R)
            ROLLBACK=1
            ;;
        *|?|h)
            echo "Usage: $0 [-a] [-u HDFS_USER] [-t TARGET_VERSION] [-p DIRECTORY_PREFIX] [-P HDFS_DIRECTORY_PREFIX] [-R] [-?]"
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
            echo "  -R              Rollback installation. Restores previously backed up versions of hadoop-azure jar file. Rollback for HDFS "
            echo "                  should follow same model as deployment."
            exit 1
            ;;
    esac
done

which jq > /dev/null
if [ $? -ne 0 ]; then

    echo "This script requires jq to run. Please install using preferred package manager"
    exit 1
fi

# Confirm rollback
if [ $ROLLBACK -gt 0 ]; then

    echo "***************** NOTICE ****************************"
    echo "This script will rollback previously applied changes."
    echo "Multiple patches and rollbacks are NOT idempotent. Rolling back after applying multiple patches "
    echo "may result in an unusable system."
    echo "Only rollback if you are confident there is only one saved original version (*.jar.original)."
    read -r -p "Are you sure you want to proceed with this operation? [y/N] " response
    if [ "${response,,}" != "y" ]; then

        exit 4
    fi
    JAR_FIND_SUFFIX="*"
fi

# If the TARGET_RELEASE hasn't been explicitly specified, try to determine this from github
if [[ -z "$TARGET_RELEASE" ]]; then

    # The value between the dollar-colon tokens is automatically substituted when committing to git.
    # Do not modify this value or the tokens
    SCRIPT_COMMIT=$(echo "$:14fd608f0ac11b27048b07c6b66d6a899934f37c:$" | cut -d '$' -f 2 | cut -d ':' -f 2)

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

if [ $ROLLBACK -eq 0 ]; then

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
fi

echo ""
echo "Locating all JAR files in $DIR_PREFIX*.tar.gz"
GZs=$(find "$DIR_PREFIX" -name "*.tar.gz" -print0 | xargs -0 zgrep "$MATCHED_JAR_FILE_NAME" | tr ":" "\n" | grep .tar.gz)
for GZ in $GZs
do

    echo $GZ
    if [ $ROLLBACK -eq 0 ]; then

        if [[ ! -e "${GZ}${BACKUP_SUFFIX}" ]]; then

            cp "$GZ" "${GZ}${BACKUP_SUFFIX}"
        fi

        ARCHIVE_DIR="${GZ}.dir"
        if [[ -d $ARCHIVE_DIR ]]; then

            rm -rf "$ARCHIVE_DIR"
        fi
        mkdir "$ARCHIVE_DIR"
        echo "    tar -C "$ARCHIVE_DIR" -zxf $GZ"
        tar -C "$ARCHIVE_DIR" -zxf "$GZ"
    else

        # Rollback changes
        if [[ -e "${GZ}${BACKUP_SUFFIX}" ]]; then

            echo "    cp ${GZ}${BACKUP_SUFFIX} $GZ"
            cp "${GZ}${BACKUP_SUFFIX}" "$GZ" 
            rm "${GZ}${BACKUP_SUFFIX}"
        fi
    fi
done

echo ""
echo "Updating all JAR files with the same name in $DIR_PREFIX$MATCHED_JAR_FILE_NAME*.jar$JAR_FIND_SUFFIX"
for DST in $(find "$DIR_PREFIX" -name "$MATCHED_JAR_FILE_NAME*.jar$JAR_FIND_SUFFIX" -a ! -name "*datalake*")
do
    echo $DST
    if [ $ROLLBACK -eq 0 ]; then

        # Backup original file (jar or symlink) if not already backed up
        if [[ ! -e "${DST}${BACKUP_SUFFIX}" ]]; then

            cp "$DST" "${DST}${BACKUP_SUFFIX}"
        fi
        # Different handling for symlink or real file
        if [[ ! -h "$DST" ]]; then

            # Replace with patched JAR
            rm -f "$DST"
            DST="$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
            echo "    cp $LOCAL_PATCH_PATH $DST"
            cp "$LOCAL_PATCH_PATH" "$DST"
        else

            # For symlink, assume the target will be replaced with the correctly named file. Just update the link.
            NEW_TARGET="$(dirname $(readlink "$DST"))/$PATCHED_JAR_FILE_NAME.jar"
            ln -sfn "$NEW_TARGET" "$DST"
        fi
    else

        # Rollback changes - need to handle 2 cases; hadoop-azure*.jar.original -> hadoop-azure*.jar & hadoop-azure*.jar -> rm
        DST_FILENAME=$(basename "$DST")
        DST_EXTENSION=.${DST_FILENAME##*.}
        if [[ "$DST_EXTENSION" == "$BACKUP_SUFFIX" ]]; then

            # hadoop-azure*.jar.original -> hadoop-azure*.jar
            DST_ORIG=$(dirname "$DST")/$(basename "$DST" $BACKUP_SUFFIX)
            echo "    cp ${DST} $DST_ORIG"
            cp "${DST}" "$DST_ORIG"
            rm "${DST}"

        elif [[ "$DST_EXTENSION" == "$JAR_EXTENSION" ]]; then

            # hadoop-azure*.jar -> rm
            echo "    rm $DST"
            rm "$DST"
        fi
    fi
done

# HDFS update
if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo ""
    echo "Updating all JAR files on HDFS matching; $HDFS_DIR_PREFIX$MATCHED_JAR_FILE_NAME*.jar$JAR_FIND_SUFFIX"
    for HDST in $(sudo -u $HDFS_USER hadoop fs -find "$HDFS_DIR_PREFIX" -name "$MATCHED_JAR_FILE_NAME*.jar$JAR_FIND_SUFFIX" | grep -v "datalake")
    do

        if [ $ROLLBACK -eq 0 ]; then

            sudo -u $HDFS_USER hadoop fs -test -e "${HDST}${BACKUP_SUFFIX}"
            if [ $? -ne 0 ]; then

                sudo -u $HDFS_USER hadoop fs -cp "$HDST" "${HDST}${BACKUP_SUFFIX}"
            fi

            sudo -u $HDFS_USER hadoop fs -rm $HDST
            HDST="$(dirname "$HDST")/$PATCHED_JAR_FILE_NAME.jar"
            echo "    hadoop fs -put -f $LOCAL_PATCH_PATH $HDST"
            sudo -u $HDFS_USER hadoop fs -put -f "$LOCAL_PATCH_PATH" "$HDST"
        else

            # Rollback changes - need to handle 2 cases; hadoop-azure*.jar.original -> hadoop-azure*.jar & hadoop-azure*.jar -> rm
            HDST_FILENAME=$(basename "$HDST")
            HDST_EXTENSION=.${HDST_FILENAME##*.}
            if [[ "$HDST_EXTENSION" == "$BACKUP_SUFFIX" ]]; then

                # hadoop-azure*.jar.original -> hadoop-azure*.jar
                HDST_ORIG=$(dirname "$HDST")/$(basename "$HDST" $BACKUP_SUFFIX)
                echo "    hadoop fs -cp $HDST $HDST_ORIG"
                sudo -u $HDFS_USER hadoop fs -cp "$HDST" "$HDST_ORIG" 
                sudo -u $HDFS_USER hadoop fs -rm "$HDST_ORIG"

            elif [[ "$HDST_EXTENSION" == "$JAR_EXTENSION" ]]; then

                # hadoop-azure*.jar -> rm
                echo "    hadoop fs -rm $HDST
                sudo -u $HDFS_USER hadoop fs -rm "$HDST"
            fi
        fi
    done
fi

if [ $ROLLBACK -eq 0 ]; then

    echo ""
    echo "Updating all .tar.gz"
    for GZ in $GZs
    do

        echo "    tar -czf $GZ -C ${GZ}.dir"
        tar -czf "$GZ" -C "${GZ}.dir" .
        rm -rf "${GZ}.dir"
    done
fi

if [ $APPLY_HDFS_PATCH -gt 0 ]; then

    echo ""
    echo "Updating all .tar.gz files on HDFS matching $HDFS_DIR_PREFIX*.tar.gz"
    for HGZ in $(sudo -E -u $HDFS_USER hadoop fs -find "$HDFS_DIR_PREFIX" -name "*.tar.gz" -print0 | xargs -0 -I % sudo -E sh -c 'hadoop fs -cat % | tar -tzv | grep "$MATCHED_JAR_FILE_NAME" && echo %' | grep ".tar.gz")
    do

        echo "$HGZ"
        if [ $ROLLBACK -eq 0 ]; then

            # Create backup
            sudo -u $HDFS_USER hadoop fs -test -e "${HGZ}${BACKUP_SUFFIX}"
            if [ $? -ne 0 ]; then

                sudo -u $HDFS_USER hadoop fs -cp "$HGZ" "${HGZ}${BACKUP_SUFFIX}"
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
                if [[ ! -e "${DST}${BACKUP_SUFFIX}" ]]; then

                    cp "$DST" "${DST}${BACKUP_SUFFIX}"
                fi
                rm -f "$DST"
                cp "$LOCAL_PATCH_PATH" "$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
            done

            cd $ARCHIVE_DIR
            tar -zcf $LOCAL_TAR_FILE *
            cd ..

            echo "    hadoop fs -copyFromLocal -p -f $LOCAL_TAR_FILE $HGZ"
            sudo -u $HDFS_USER hadoop fs -copyFromLocal -p -f "$LOCAL_TAR_FILE" "$HGZ"
            rm -rf $ARCHIVE_DIR
            rm -f $LOCAL_TAR_FILE
        else

            # Rollback changes
            sudo -u $HDFS_USER hadoop fs -test -e "${HGZ}${BACKUP_SUFFIX}"
            if [ $? -eq 0 ]; then

                echo "    hadoop fs -cp ${HGZ}${BACKUP_SUFFIX} ${HGZ}"
                sudo -u $HDFS_USER hadoop fs -cp "${HGZ}${BACKUP_SUFFIX}" "$HGZ" 
                sudo -u $HDFS_USER hadoop fs -rm "${HGZ}${BACKUP_SUFFIX}"
            fi
        fi
    done
fi
echo "Finished"
