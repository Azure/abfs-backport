#!/bin/bash

# Script to patch legacy Hadoop with new ABFS driver (hadoop-azure.*.jar) that has been specifically backported
# for the targeted Hadoop distro and version.
# To fully patch a cluster, this script must be run on EVERY node to update the local filesystem. On ONE NODE ONLY
# the -a switch must be specified to patch the HDFS contents.

which jq > /dev/null
if [ $? -ne 0 ]; then

    echo "This script requires jq to run. Please install using preferred package manager"
    exit 1
fi


# Parameters
APPLY_HDFS_PATCH=0
HDFS_USER=
DIR_PREFIX=
HDFS_DIR_PREFIX=
ROLLBACK=0


# Constants
export MATCHED_JAR_FILE_NAME=hadoop-azure
GITHUB_API_ROOT_URI=https://api.github.com/repos/jamesbak/abfs_backport
CURR_TIME=$(date "+%Y-%m-%d-%H-%M-%S")
BACKUP_SUFFIX=".original_${CURR_TIME}"
JAR_EXTENSION=".jar"
#
JAR_FIND_SUFFIX=""


while getopts ":a?hu:p:P:R:" options
do
    case "${options}" in
        a)
            APPLY_HDFS_PATCH=1
            ;;
        u)
            HDFS_USER=${OPTARG}
            ;;
        p)
            DIR_PREFIX=${OPTARG}
            ;;
        P)
            HDFS_DIR_PREFIX=${OPTARG}
            ;;
        R)
            ROLLBACK=1
            if [ -z ${OPTARG} ]; then
                echo "Exiting; Backup version parameter is required for rollback"
                exit 4
            fi
            BACKUP_SUFFIX=".original_${OPTARG}"
            ;;
        *|?|h)
            echo "Usage: $0 [-a] [-u HDFS_USER] [-t TARGET_VERSION] [-p DIRECTORY_PREFIX] [-P HDFS_DIRECTORY_PREFIX] [-R] [-?]"
            echo ""
            echo "Where:"
            echo "  -a              Update HDFS contents. This switch should only be specified for ONE node in a cluster patch."
            echo "  -u HDFS_USER    Specifies the user name with superuser privileges on HDFS. Applicable only if the -a switch is specified."
            echo "  -p DIRECTORY_PREFIX "
            echo "                  Specifies a prefix that is specific to the Hadoop distro & version to search for files to patch."
            echo "  -P HDFS_DIRECTORY_PREFIX "
            echo "                  Specifies a prefix that is specific to the Hadoop distro & version to search on HDFS for files to patch."
            echo "  -R              Rollback installation. Restores previously backed up versions of hadoop-azure jar file. Rollback for HDFS "
            echo "                  should follow same model as deployment. Specify the backup version for the rollback. Ex: Specify 2020-06-07-10-10-10 "
            echo "                  for the backup file named hadoop-azure.*.jar.original_2020-06-07-10-10-10"
            exit 1
            ;;
    esac
done


[[ "${DIR_PREFIX}" != */ ]] && DIR_PREFIX="${DIR_PREFIX}/"
[[ "${HDFS_DIR_PREFIX}" != */ ]] && HDFS_DIR_PREFIX="${HDFS_DIR_PREFIX}/"

TARGET_RELEASE="HDP-2.5.2"


# Confirm rollback
if [ $ROLLBACK -gt 0 ]; then
    
    echo "find $DIR_PREFIX -name $MATCHED_JAR_FILE_NAME*.jar$BACKUP_SUFFIX -a ! -name *datalake* | wc -l"
    JARCOUNT=$(find $DIR_PREFIX -name $MATCHED_JAR_FILE_NAME*.jar$BACKUP_SUFFIX -a ! -name *datalake* | wc -l)
    echo "jar files found with rollback version: $JARCOUNT"

    echo "find $DIR_PREFIX -name *.tar.gz${BACKUP_SUFFIX} -a ! -name *datalake* | wc -l"
    GZCOUNT=$(find $DIR_PREFIX -name ${GZ}${BACKUP_SUFFIX} -a ! -name *datalake* | wc -l)
    echo "Zip files found with rollback version: $GZCOUNT"

    TOT=$(($JARCOUNT+$GZCOUNT))
    echo "Number of files found for rollback : $TOT"
    if [[ ${TOT} -eq 0 ]]; then
        echo "Exiting. Backup version for rollback specified is not found."
        exit 4
    fi

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


RELEASE_INFO=$(curl "${GITHUB_API_ROOT_URI}/releases/tags/${TARGET_RELEASE}")
JAR_ASSET=$(echo $RELEASE_INFO | jq -r '.assets[] | select(.content_type == "application/java-archive") | .')
if [[ -z "$JAR_ASSET" ]]; then

    echo "Unable to get information for .jar file associated with $TARGET_RELEASE release."
    exit 4
fi
PATCHED_JAR_FILE_NAME=$(basename $(echo $JAR_ASSET | jq -r '.name') .jar)
REMOTE_PATCH_PATH=$(echo $JAR_ASSET | jq -r '.browser_download_url')
LOCAL_PATCH_PATH="/tmp/$PATCHED_JAR_FILE_NAME.new"

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

        # Different handling for symlink or real file
        if [[ ! -h "$DST" ]]; then

            # Backup original file (jar or symlink) if not already backed up
            if [[ ! -e "${DST}${BACKUP_SUFFIX}" ]]; then

                cp "$DST" "${DST}${BACKUP_SUFFIX}"
            fi

            # Replace with patched JAR
            rm -f "$DST"
            DST="$(dirname "$DST")/$PATCHED_JAR_FILE_NAME.jar"
            echo "    cp $LOCAL_PATCH_PATH $DST"
            cp "$LOCAL_PATCH_PATH" "$DST"
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
                echo "    hadoop fs -rm $HDST"
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

