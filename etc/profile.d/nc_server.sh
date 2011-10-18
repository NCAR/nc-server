if ! echo ${PATH} | /bin/grep -q /opt/nc_server/bin ; then
    # Uncomment this if you want nc_server programs in PATH for all login users
    # PATH=${PATH}:/opt/nc_server/bin
    :
fi
