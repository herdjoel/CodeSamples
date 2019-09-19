## Version: Python 3.6; ArcGIS Pro 2.3
## Last updated 9/18/2019
## Description:
### Update process for static Feature/Vector Tile services in ArcGIS Portal
### Reads from config (.ini) file to get details of service

import os, sys, datetime, traceback, arcpy, shutil, time, json, urllib, socket, zipfile
import multiprocessing as mp
from arcgis.gis import GIS

debug = False   # True for local Development/Testing
rebuild_data = True

# Database Connections
spuuser = "*****"
gisuser = "*****"
gisuserimgp = "*****"
crw_prod = "*****"
crw_test = "*****"
crw_dev = "*****"
crw_spugis = "*****"

# Address for Alert Emails
from_address = "no_reply_agol_update@seattle.gov"  # fromAddress for email

# ID of City of Seattle ArcGIS Online
agol_id = "******"


# Primary workflow controller
def main():
    init_sources()
    
    # Allowing edit of global variables 
    global currentLogFileName, cur_log_file_path
    
    # Set timer for total runtime
    total_time = delta_time_system_timer(0)

    # Logging to Text file on Network Drive
    write_to_log(log_file, "")
    write_to_log(log_file, "Begin Update Pipeline", True)
    write_to_log(log_file, "")
    write_to_log(log_file, f"Script Source Path: {local_path}")
    write_to_log(log_file, f"Target Path: {list_path}")
    
    # Create TO_DELETE folder in network directory if it doesn't exist
    # For Temporary Log Files
    if not os.path.exists(temp_folder):
        write_to_log(log_file, "Creating TO_DELETE directory")
        os.mkdir(temp_folder)
    else:
        shutil.rmtree(temp_folder)
        os.mkdir(temp_folder)
    
    # Move all old files to History (network directory)
    files_in_data = os.listdir(data_path)
    if rebuild_data:
        if not os.path.exists(history_path):
            os.mkdir(history_path)
        else:
            shutil.rmtree(history_path)
            os.mkdir(history_path)
        for data in files_in_data:
            if data != "History":
                file_path = f"{data_path}\\{data}"
                if data == "Index":
                    shutil.rmtree(file_path)
                else:
                    new_file_path = f"{history_path}\\{data}"
                    try:
                        shutil.copyfile(file_path, new_file_path)
                        os.remove(file_path)
                    except Exception:
                        try:
                            shutil.copytree(file_path, new_file_path, ignore_dangling_symlinks=True)
                            shutil.rmtree(file_path)
                        except Exception:
                            pass
                        
        # Copy Data to local folder from network directory
        if os.path.exists(local_data_path):
            shutil.rmtree(local_data_path)
        try:
            shutil.copytree(data_path, local_data_path, ignore_dangling_symlinks=True)
        except Exception:
            pass
    
    # Begin iterating through .ini files in target folder
    write_to_log(log_file, "Iterating through Target Path", True)
    write_to_log(log_file, "")
    
    log_list = []
    failed_log_list = {}
    index = 0
    list_of_files = os.listdir(list_path)
    
    for file_name in list_of_files:
        start_timestamp = delta_time_system_timer(0)
        result = 0
        
        try:
            init_dict = {}
            file_path = f"{list_path}\\{file_name}"
            
            # Open .ini file and read all arguments out
            with open(file_path, "r") as init_file:
                line = init_file.readline()
                
                while line:  # While Not Null
                    split_line = line.split('=')
                    if len(split_line) > 1:
                        init_dict[split_line[0].strip()] = split_line[1].strip()
                    line = init_file.readline()
            
            # Check that the .ini file is not empty
            if len(init_dict.keys()) > 0:
                service_type = init_dict['SERVICETYPE']  # Feature, Vector Tile, Tile, Map Image
                service_name = init_dict['SERVICENAME']
                currentLogFileName = f"{service_name}_{month_day_year}.txt"
                cur_log_file_path = f"{temp_folder}\\{currentLogFileName}"
                
                write_to_log(log_file, f"Opening {file_name}", False, cur_log_file_path)
                write_to_log(log_file, f"Hostname: {socket.getfqdn()}", False, cur_log_file_path)
                write_to_log(log_file, f"Updating {service_name} as a {service_type}.", False, cur_log_file_path)
                
                # Handle as Feature Service
                if service_type.upper() == "FEATURE":
                    result = hosted_feature_update(init_dict)
                
                # Handle as Vector Tile Service
                elif service_type.upper() == "VECTOR TILE":
                    result = vector_tile_update(init_dict)
                    if result > 1:
                        write_to_log(log_file, "Retrying due to failed run.", False, cur_log_file_path)
                        write_to_log(log_file, "")
                        result = vector_tile_update(init_dict)
                
                # Handle as neither Feature or Tile Service
                else:
                    write_to_log(log_file, f"{service_name} has an invalid Service Type of: {service_type}", False, cur_log_file_path)
                
                # Write runtime for this service to log
                if start_timestamp is not None:
                    seconds, minutes, hours = delta_time_system_timer(start_timestamp)
                    write_to_log(log_file, f"Runtime: {hours} hours, {minutes} minutes, {seconds} seconds.", False, cur_log_file_path)
                write_to_log(log_file, f"Summary Log File: {log_file}.", False, cur_log_file_path)
                write_to_log(log_file, "")
                
                # Send Email of final result if process failed
                if result >= 1:
                    failed_log_list[index] = [cur_log_file_path, service_name]
                    index += 1
                
                # Continue if successful
                elif result == 0:
                    log_list.append(cur_log_file_path)
        
        except Exception:
            throw_exception(log_file)
    
    # Copy data back to Network Drive
    write_to_log(log_file, "Copying data from local to network drive.", True)

    if rebuild_data:
        # Remove Old Data from network
        if os.path.exists(data_path):
            shutil.rmtree(data_path)
        try:
            shutil.copytree(local_data_path, data_path, ignore_dangling_symlinks=True)
        except Exception:
            pass

        # Copy new local data to network drive
        if not os.path.exists(data_path):
            try:
                shutil.copytree(local_data_path, data_path, ignore_dangling_symlinks=True)
            except Exception:
                pass
    
    # Send final result email
    write_to_log(log_file, "Sending Result Emails.", True)
    if len(log_list) > 0:
        send_email(True, "", log_list)
    if len(failed_log_list) > 0:
        for ind in failed_log_list:
            send_email(False, failed_log_list[ind][0], None, failed_log_list[ind][1])
    
    write_to_log(log_file, "Deleting Temporary Data.", True)
    # Delete all temp logs for emails
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)
    
    # Write final runtime to log
    if total_time is not None:
        seconds, minutes, hours = delta_time_system_timer(total_time)
        write_to_log(log_file, f"Total Runtime: {hours} hours, {minutes} minutes, { seconds} seconds.", False)
    write_to_log(log_file, "End Update Pipeline", True)
    write_to_log(log_file, "", False)
    write_to_log(log_file, "----------------------------------------------------------------------------------", False)
    

# Initiate global variables 
def init_sources():
    global log_file, list_path, month_day_year, data_path, log_path, local_data_path, local_path, \
        target_folder_path, portal_name, folder_name_global, temp_folder, history_path, local_hisotry_path
    
    local_path = os.getcwd()
    month_day_year = f"{datetime.datetime.now().strftime('%m_%d_%Y')}"
    
    crash_file = f"{local_path}\\CRASH_{month_day_year}.txt"
    if os.path.exists(crash_file):
        os.remove(crash_file)
    
    try:
        if debug is False:
            target_folder_path = str(sys.argv[1])
        else:
            target_folder_path = "***************************"
        
        if target_folder_path is None or target_folder_path == "":
            write_to_log(crash_file, "ERROR: No Target Folder Path.", True)
            sys.exit()
            
        else:
            list_path = f"{target_folder_path}\\list"
            data_path = f"{target_folder_path}\\data"
            log_path = f"{target_folder_path}\\logs"
            temp_folder = f"{target_folder_path}\\TO_DELETE"
            history_path = f"{target_folder_path}\\data\\history"
            
            if not os.path.exists(log_path):
                os.mkdir(log_path)
            log_file = f"{log_path}\\log_portal_update_{month_day_year}.txt"
            if not os.path.exists(data_path):
                os.mkdir(data_path)
            if not os.path.exists(history_path):
                os.mkdir(history_path)
    
            local_data_path = f"{local_path}\\data"
            local_hisotry_path = f"{local_data_path}\\history"
            split_path = target_folder_path.split("\\")
            portal_name = split_path[-2]
            folder_name_global = split_path[-1]
            
    except Exception:
        throw_exception(crash_file)


# Publish/Update Vector Tile Service
def vector_tile_update(init_dict):
    global cur_log_file_path
    
    try:
        write_to_log(log_file, "Begin", True)
        
        # Get config arguments from .ini
        service_name = init_dict['SERVICENAME']
        service_id = init_dict['SERVICEID']
        folder_name = init_dict['FOLDERNAME']
        project_path = init_dict['APRX']
        tags = init_dict['TAGS']
        description = init_dict['DESCRIPTION']
        copy_data = init_dict['COPYDATA']
        editing = init_dict['EDITING']
        exporting = init_dict['EXPORTING']
        sync = init_dict['SYNC']
        everyone = init_dict['EVERYONE']
        org = init_dict['ORG']
        groups = init_dict['GROUPS']
        portal_url = init_dict['PORTALURL']
        admin_user = init_dict['ADMINUSER']
        admin_pass = init_dict['ADMINPASS']
        max_cache = init_dict['MAXCACHE']
        
        # Set sharing options
        shrOrg = True if org.upper() == 'TRUE' else False
        shrEveryone = True if everyone.upper() == 'TRUE' else False
        groups = None if groups.upper() == "NONE" else [group.strip() for group in groups.split(',')]
        folder_name = None if folder_name.upper() == "NONE" else folder_name
        copy_data = True if copy_data.upper() == 'TRUE' else False
        editing = True if editing.upper() == "TRUE" else False
        exporting = True if exporting.upper() == 'TRUE' else False
        sync = True if sync.upper() == 'TRUE' else False

        # Assign VTPK name
        pckg_name = f"{service_name}_{month_day_year}"
        pckg_name_vtpk = f"{pckg_name}.vtpk"
        temp_name = f"temp_{service_name[:12]}"
        pckg_name_vtpk_path = os.path.join(local_data_path, pckg_name_vtpk)

        # Define Portal connection
        if "arcgis" in portal_url:
            target_portal = f"https://{portal_url}"
        else:
            target_portal = f"https://{portal_url}/portal"

        if rebuild_data:
            # Delete vtpk if already exists
            if os.path.exists(pckg_name_vtpk_path):
                os.remove(pckg_name_vtpk_path)
        
        # Pull Map from project
        prj = arcpy.mp.ArcGISProject(project_path)
        mp = prj.listMaps()[0]
        write_to_log(log_file, f"Source Project: {project_path}", False, cur_log_file_path)

        # Create VTPK from Map
        write_to_log(log_file, f"Creating {pckg_name_vtpk}", True)
        sp_tiling_scheme = os.path.join(local_path, "tiling_scheme_SP.xml")
        max_cached_scale = int(max_cache)
        if rebuild_data:
            try:
                arcpy.CreateVectorTilePackage_management(in_map=mp,
                                                         output_file=pckg_name_vtpk_path,
                                                         service_type="EXISTING",
                                                         tiling_scheme=sp_tiling_scheme,
                                                         max_cached_scale=max_cached_scale,
                                                         tile_structure='INDEXED',
                                                         summary=description,
                                                         tags=tags)
            except Exception:  # Fail safe
                time.sleep(60)  # Give time if Portal is lagging
                arcpy.CreateVectorTilePackage_management(in_map=mp,
                                                         output_file=pckg_name_vtpk_path,
                                                         service_type="EXISTING",
                                                         tiling_scheme=sp_tiling_scheme,
                                                         max_cached_scale=max_cached_scale,
                                                         tile_structure='INDEXED',
                                                         summary=description,
                                                         tags=tags)

        # Close connection to Project
        del prj

        # Login to ArcGIS Portal
        gis = GIS(target_portal, admin_user, admin_pass)
        write_to_log(log_file, "Active Portal: {gis.url}", True, cur_log_file_path)

        # Delete VTPK if exists in Portal
        write_to_log(log_file, "Clear Service Namespace", True, cur_log_file_path)

        # Delete Temp package if exists
        search_query = f"title:{temp_name} AND owner:{admin_user}"
        for temp_vtpk in gis.content.search(search_query, item_type="Vector Tile Package"):
            try:
                temp_vtpk.delete()
            except Exception:
                time.sleep(60)  # Give time if portal is lagging
                temp_vtpk.delete()  # Try again
        # Delete Temp Service if exists
        for temp_serv in gis.content.search(search_query, item_type="Vector Tile Service"):
            try:
                temp_serv.delete()
            except Exception:
                time.sleep(60)  # Give time if portal is lagging
                temp_serv.delete()  # Try again
        search_query = f"title:{pckg_name} AND owner:{admin_user}"
        for old_vtpk in gis.content.search(search_query, item_type="Vector Tile Package"):
            if old_vtpk.title == pckg_name and old_vtpk:
                try:
                    old_vtpk.delete()
                except Exception:
                    time.sleep(60)  # Give time if portal is lagging
                    old_vtpk.delete()  # Try again
        for old_serv in gis.content.search(search_query, item_type="Vector Tile Service"):
            if old_serv.title == pckg_name and old_serv:
                try:
                    old_serv.delete()
                except Exception:
                    time.sleep(60)  # Give time if portal is lagging
                    old_serv.delete()  # Try again

        # Add VTPK to Portal
        write_to_log(log_file, "Staging Vector Tile into Portal", True, cur_log_file_path)
        try:
            vtpk = gis.content.add(item_properties={'type': "Vector Tile Package", "description": description,
                                                    "tags": tags}, data=pckg_name_vtpk_path, folder=folder_name)
        except Exception:
            time.sleep(60)  # Give time if portal is lagging
            vtpk = gis.content.add(item_properties={'type': "Vector Tile Package", "description": description,
                                                    "tags": tags}, data=pckg_name_vtpk_path, folder=folder_name)

        # Publish New Service
        write_to_log(log_file, "Creating New Service", True, cur_log_file_path)
        try:
            final_service = vtpk.publish()
        except Exception:
            time.sleep(60)  # Give time if portal is lagging
            final_service = vtpk.publish()  # Try Again

        write_to_log(log_file, f"Tile Service: {final_service.homepage}.", False, cur_log_file_path)

        # Update Style File
        update_style_file(service_name, admin_user, gis, pckg_name, pckg_name_vtpk_path, portal_url, final_service)

        # Update Sharing Settings
        try:
            final_service.share(org=shrOrg, everyone=shrEveryone, groups=groups)
        except Exception:
            time.sleep(60)  # Give time if portal is lagging
            final_service.share(org=shrOrg, everyone=shrEveryone, groups=groups)  # Try Again

        return 0
    
    except Exception:
        throw_exception(log_file, "", cur_log_file_path)
        return 1
        

# Update style file service
def update_style_file(usf_service_name: str, usf_admin_user: str, usf_gis, usf_pckg_name: str,
                      usf_pckg_name_vtpk_path: str, usf_portal_url: str, usf_final_service):
    style_service_name = f"{usf_service_name}_Style"
    style_service = None
    search_query = f"title:{style_service_name} AND owner:{usf_admin_user}"
    for service in usf_gis.content.search(search_query, item_type="Vector Tile Service"):
        if service.title == style_service_name and service:
            style_service = service

    # Update Style File
    style_folder = f"{local_data_path}\\{usf_pckg_name}"
    if os.path.exists(style_folder):
        shutil.rmtree(style_folder)
    os.mkdir(style_folder)

    # Extract vtpk to zip
    new_vtpk_path = f"{local_data_path}\\{usf_pckg_name}.zip"
    os.rename(usf_pckg_name_vtpk_path, new_vtpk_path)

    # Unzip
    zip_ref = zipfile.ZipFile(new_vtpk_path, 'r')
    zip_ref.extractall(style_folder)
    zip_ref.close()

    os.rename(new_vtpk_path, usf_pckg_name_vtpk_path)

    # Repoint Style Path to unzipped folder
    style_file_path = f"{style_folder}\\p12\\resources\\styles\\root.json"

    # Edit paths in style file
    if "arcgis" in usf_portal_url:
        style_target_portal = f"https://tiles.arcgis.com/tiles/{agol_id}/arcgis/rest/services/"
    else:
        style_target_portal = f"https://{usf_portal_url}/server/rest/services/Hosted/"
    service_path = style_target_portal + usf_pckg_name

    # Write changes to Style File
    with open(style_file_path, "r") as style_file_edits:
        data = json.load(style_file_edits)
    data["sprite"] = f"{service_path}/VectorTileServer/resources/styles/../sprites/sprite"
    data["glyphs"] = service_path + "/VectorTileServer/resources/styles/../fonts/{fontstack}/{range}.pbf"
    data["sources"]["esri"]["url"] = f"{service_path}/VectorTileServer"
    with open(style_file_path, "w") as style_file_edits:
        json.dump(data, style_file_edits)

    # Update Style File in final_service
    if not style_service:
        style_service = usf_final_service.copy(style_service_name)
    try:
        style_service.resources.add(file=style_file_path, folder_name="styles", file_name="root.json")
    except:
        style_service.resources.update(file=style_file_path, folder_name="styles", file_name="root.json")

    write_to_log(log_file, f"Style Service: {style_service.homepage}.", True, cur_log_file_path)


# Publish/Update Hosted Feature Service
def hosted_feature_update(init_dict):
    try:
        final_result = 0
        service_name = init_dict['SERVICENAME']
        service_id = init_dict['SERVICEID']
        folder_name = init_dict['FOLDERNAME']
        project_path = init_dict['APRX']
        tags = init_dict['TAGS']
        description = init_dict['DESCRIPTION']
        copy_data = init_dict['COPYDATA']
        editing = init_dict['EDITING']
        exporting = init_dict['EXPORTING']
        sync = init_dict['SYNC']
        everyone = init_dict['EVERYONE']
        org = init_dict['ORG']
        groups = init_dict['GROUPS']
        portal_url = init_dict['PORTALURL']
        admin_user = init_dict['ADMINUSER']
        admin_pass = init_dict['ADMINPASS']
        
        # Set sharing options
        shrOrg = True if org.upper() == 'TRUE' else False
        shrEveryone = True if everyone.upper() == 'TRUE' else False
        groups = None if groups.upper() == "NONE" else [group.strip() for group in groups.split(',')]
        folder_name = None if folder_name.upper() == "NONE" else folder_name
        copy_data = True if copy_data.upper() == 'TRUE' else False
        editing = True if editing.upper() == "TRUE" else False
        exporting = True if exporting.upper() == 'TRUE' else False
        sync = True if sync.upper() == 'TRUE' else False
        
        # Declare paths
        # Set up Portal connection
        if "arcgis" in portal_url:
            target_portal = f"https://{portal_url}"
        else:
            target_portal = f"https://{portal_url}/portal"
        aprx_name = f"{service_name}.aprx"
        gdb_dir = os.path.join(local_data_path, f"{service_name}_data")
        aprx_path = os.path.join(local_data_path, aprx_name)

        if rebuild_data:
            # Delete old data
            if os.path.exists(gdb_dir):
                shutil.rmtree(gdb_dir)
            os.makedirs(gdb_dir)
        
        write_to_log(log_file, "BEGIN", True, cur_log_file_path)

        # Stage Multiprocessing Manager
        m = mp.Manager()
        fc_relationships = m.dict()
        
        write_to_log(log_file, f"Source Project: {project_path}", False, cur_log_file_path)

        if rebuild_data:
            # Delete/Copy Local Project for refresh
            if os.path.exists(aprx_path):
                os.remove(aprx_path)
            shutil.copy(project_path, aprx_path)

            proc_count = int(mp.cpu_count() / 2)
            if debug or proc_count == 1:
                save_to_gdb_aprx([0, service_name, 1, fc_relationships, log_file, local_data_path, aprx_path,
                                  cur_log_file_path])
            else:
                write_to_log(log_file, "Preprocessing layers with Multiprocessing", False)
                write_to_log(log_file, f"{proc_count} usable cores", False)

                # Get Create copies of all feature classes into local GDBs
                with mp.Pool(processes=proc_count, initializer=init_sources) as pool:
                    for i in pool.imap_unordered(save_to_gdb_aprx,
                                                 [(proc_num, service_name, proc_count, fc_relationships,
                                                   log_file, local_data_path, aprx_path, cur_log_file_path)
                                                  for proc_num in range(0, proc_count)]):
                        if i == 1:
                            final_result = 1

            write_to_log(log_file, f"Making Changes to: {aprx_path}", True)

            layers = []
            proj = arcpy.mp.ArcGISProject(aprx_path)
            mapprj = proj.listMaps()[0]
            aprx_layer_list = mapprj.listLayers()
            for lyr in aprx_layer_list:
                try:
                    layers.append(str(lyr))
                except Exception:
                    continue
            del proj

            # Update all layers in Project to point to local GDBs
            if debug or proc_count == 1:
                update_project([0, fc_relationships, aprx_path, layers, log_file, proc_count, cur_log_file_path])
            else:
                lock = mp.Lock()
                with mp.Pool(processes=proc_count, initializer=init_lock, initargs=(lock,)) as pool:
                    for i in pool.imap_unordered(update_project,
                                                 [(proc_num, fc_relationships, aprx_path, layers, log_file,
                                                   proc_count, cur_log_file_path)
                                                  for proc_num in range(0, proc_count)]):
                        if i == 1:
                            final_result = 1

        write_to_log(log_file, "Publishing Project to Portal", True, cur_log_file_path)
        write_to_log(log_file, f"Connecting to {portal_url} as {admin_user}.", False, cur_log_file_path)

        arcpy.SignInToPortal(target_portal, admin_user, admin_pass)

        # Open Project for editing
        prj = arcpy.mp.ArcGISProject(aprx_path)
        mapprj = prj.listMaps()[0]

        write_to_log(log_file, "Creating Service Definition", True, cur_log_file_path)

        sd_name = f"{service_name}_{month_day_year}"
        sd_name_sddraft = f"{sd_name}.sddraft"
        sd_name_sd = f"{sd_name}.sd"
        sd_name_sddraft_path = os.path.join(local_data_path, sd_name_sddraft)
        sd_name_sd_path = os.path.join(local_data_path, sd_name_sd)

        if rebuild_data:
            # Create SDDraft, delete if exists
            if os.path.exists(sd_name_sddraft_path):
                os.remove(sd_name_sddraft_path)
            if os.path.exists(sd_name_sd_path):
                os.remove(sd_name_sd_path)

            sharing_draft = mapprj.getWebLayerSharingDraft("HOSTING_SERVER", "FEATURE", service_name)
            sharing_draft.tags = tags
            sharing_draft.description = description
            sharing_draft.allowExporting = exporting
            sharing_draft.overwriteExistingService = True
            sharing_draft.portalFolder = folder_name

            # Create Service Definition Draft file
            sharing_draft.exportToSDDraft(sd_name_sddraft_path)

            # Stage Service SDDraft -> SD file
            write_to_log(log_file, "Finalizing Service (.sddraft to .sd)", True)
            try:
                arcpy.StageService_server(sd_name_sddraft_path, sd_name_sd_path)
            except Exception:
                arcpy.StageService_server(sd_name_sddraft_path, sd_name_sd_path)

            # Delete SDDraft
            if os.path.exists(sd_name_sddraft_path):
                os.remove(sd_name_sddraft_path)

        # Sign into portal
        gis = GIS(target_portal, admin_user, admin_pass)
        write_to_log(log_file, f"Active Portal: {gis.url}", True, cur_log_file_path)
        
        # Delete Service Definition if Exists
        search_query = f"title:{sd_name} AND owner:{admin_user}"
        for item in gis.content.search(search_query, item_type="Service Definition"):
            if item.title == sd_name and item:
                try:
                    item.delete()
                except Exception:
                    item.delete()

        # Add Service Definition to Portal
        sdItem = None
        write_to_log(log_file, "Adding Service Definition to Portal", True, cur_log_file_path)
        while not gis.content.search(search_query, item_type="Service Definition"):
            try:
                sdItem = gis.content.add(item_properties={'type': "Service Definition"}, data=sd_name_sd_path,
                                         folder=folder_name)
            except Exception as e:
                pass

        if not sdItem:
            sdItem = gis.content.search(search_query, item_type="Service Definition")[0]

        # Clean up Data Connections
        del prj

        # Publish Feature Service
        feature_service = publish_as_overwrite_feature_service(service_id, service_name, gis, log_file,
                                                               cur_log_file_path, sdItem)
        # Update Share Settings
        if feature_service:
            try:
                feature_service.share(org=shrOrg, everyone=shrEveryone, groups=groups)
            except Exception as e:
                time.sleep(60)  # Give time if portal is lagging
                feature_service.share(org=shrOrg, everyone=shrEveryone, groups=groups)
        else:
            write_to_log(log_file, "ERROR: Failed to publish service", False, cur_log_file_path)
            return 1

        # Update Feature Service Title
        try:
            feature_service.update(item_properties={"title": service_name})
        except Exception as e:
            time.sleep(60)  # Give time if portal is lagging
            feature_service.update(item_properties={"title": service_name})

        write_to_log(log_file, f"Feature Service URL: {feature_service.homepage}.", False, cur_log_file_path)
        write_to_log(log_file, "Feature Service Published.", True, cur_log_file_path)
        return final_result

    except Exception as e:
        throw_exception(log_file, "", cur_log_file_path)
        return 1


# Overwrite the underlying data in the target Feature service
def publish_as_overwrite_feature_service(pofs_service_id: str, pofs_service_name: str, pofs_gis, pofs_log_file: str,
                                         pofs_cur_log_file_path: str, pofs_sd_item):
    feature_service = None

    # Toggle Search by ID or Search by Title
    search_by_id = False
    if pofs_service_id.upper() != 'NONE':
        search_by_id = True
        search_query = "id:{0}".format(pofs_service_id)
    else:
        search_query = "title:{0} OR name:{0}".format(pofs_service_name)

    # Make sure that there's a Feature Service to replace
    replace_sdItem = None
    publish_params = {"title": pofs_service_name}
    if search_by_id:
        replace_sdItem = pofs_gis.content.search(search_query, item_type="Feature Service")[0]
    if not replace_sdItem:
        print(f"Searching for matching titles... current search: {pofs_service_name}")
        for item in pofs_gis.content.search(search_query, item_type="Feature Service"):
            if (item.title == pofs_service_name or item.title == pofs_sd_item.title) and item and not replace_sdItem:
                replace_sdItem = item
    if replace_sdItem:
        write_to_log(pofs_log_file, "To Be Replaced Service: {0}".format(replace_sdItem.homepage), False,
                     pofs_cur_log_file_path)
        write_to_log(pofs_log_file, "Overwriting Feature Service", True, pofs_cur_log_file_path)
        try:
            feature_service = pofs_sd_item.publish(overwrite=True, file_type='serviceDefinition')
            tries = 30
        except Exception as e:
            throw_exception(pofs_log_file, "", pofs_cur_log_file_path)
    else:
        feature_service = publish_as_new_feature_service(pofs_sd_item, pofs_log_file, pofs_cur_log_file_path)

    return feature_service


# Publish a new feature service
def publish_as_new_feature_service(pnfs_sdItem, pnfs_log: str, pnfs_curlog: str):
    write_to_log(pnfs_log, "Publishing Feature Service", True, pnfs_curlog)
    pnfs_feature_service = None
    try:
        pnfs_feature_service = pnfs_sdItem.publish(file_type='serviceDefinition')
    except Exception as e:
        time.sleep(60)  # Give time if portal is lagging
        pnfs_feature_service = pnfs_sdItem.publish(file_type='serviceDefinition')
    return pnfs_feature_service


# Save layers in service to local file geodatabase
def save_to_gdb_aprx(args):
    global spuuser, gisuser, gisuserimgp
    
    increment = args[0]
    cur_service_name = args[1]
    cur_proc_count = args[2]
    cur_fc_relationships = args[3]
    cur_logFile = args[4]
    cur_data_path = args[5]
    cur_aprx_path = args[6]
    currentLogFilePath = args[7]
    cur_layer = ""
    
    try:
        cur_gdb_dir = os.path.join(cur_data_path, f"{cur_service_name}_data")
        gdb_name = f"{cur_service_name}_{increment}.gdb"
        gdb_path = os.path.join(cur_gdb_dir, gdb_name)
        arcpy.CreateFileGDB_management(cur_gdb_dir, gdb_name)
        
        # Get working projects
        replacement_prj = arcpy.mp.ArcGISProject(cur_aprx_path)
        
        # Pull Map
        replacement_mp = replacement_prj.listMaps()[0]
        
        # Pull Layers
        layer_list = replacement_mp.listLayers()
        layer_list_length = len(layer_list)
        # To account for the number of processes, to make sure the range gets all the layers
        range_length = layer_list_length + (layer_list_length % cur_proc_count)
        
        for i in range(0, range_length, cur_proc_count):
            index = i + increment
            if index < layer_list_length:
                cur_layer = layer_list[index]
                if cur_layer.isGroupLayer is False:
                    cont = True
                    try:
                        if cur_layer.isFeatureLayer:
                            cont = True
                    except Exception:
                        cont = False
                    if cont is True:
                        if cur_layer.isBasemapLayer is False and cur_layer.isWebLayer is False:
                            # Make local copy of gdb with filtered results
                            definition_query = ""
                            if cur_layer.supports("DEFINITIONQUERY") and \
                                    len(cur_layer.definitionQuery) > 0 and \
                                    cur_layer.definitionQuery != "":
                                definition_query = cur_layer.definitionQuery
                            if cur_layer.connectionProperties:
                                connectionProperties = cur_layer.connectionProperties
                                if connectionProperties['connection_info']:
                                    if 'user' in connectionProperties['connection_info']:
                                        user = connectionProperties['connection_info']['user']
                                        dataset = connectionProperties['dataset']
                                        datasource = cur_layer.dataSource
                                        if len(datasource) > 150:
                                            time.sleep(2)
                                            datasource = cur_layer.dataSource
                                        datasource_dict = {}
                                        ds_config = datasource.split(',')
                                        for conn_string in ds_config:
                                            ds_key_value = conn_string.split('=')
                                            try:
                                                datasource_dict[ds_key_value[0].upper()] = ds_key_value[1]
                                            except:
                                                pass
                                        try:
                                            edit_dataset = datasource_dict["DATASET"]
                                        except:
                                            edit_dataset = datasource[datasource.rfind("\\") + 1:]
                                        server = connectionProperties['connection_info']['server']
                                        instance = connectionProperties['connection_info']['instance']
                                        if user == 'gisuser' and (server == 'spugisp.world'
                                                                  or instance == 'sde:oracle$sde:oracle11g:spugisp'
                                                                  or instance == 'sde:oracle$sde:oracle11g:spugisp.world'):
                                            sde_conn = gisuser
                                        elif user == 'spuuser' and (server == 'spugisp.world'
                                                                    or instance == 'sde:oracle$sde:oracle11g:spugisp'
                                                                    or instance == 'sde:oracle$sde:oracle11g:spugisp.world'):
                                            sde_conn = spuuser
                                        elif user == 'crw' and (server == 'spugisp.world'
                                                                or instance == 'sde:oracle$sde:oracle11g:spugisp'
                                                                or instance == 'sde:oracle$sde:oracle11g:spugisp.world'):
                                            sde_conn = crw_spugis
                                        elif user == 'gisuser' and server == 'spuimgp.world':
                                            sde_conn = gisuserimgp
                                        elif user == 'crw' and 'shedsded' in instance:
                                            sde_conn = crw_dev
                                        elif user == 'crw' and 'shedsdet' in instance:
                                            sde_conn = crw_test
                                        elif user == 'crw':
                                            sde_conn = crw_prod
                                        else:
                                            sde_conn = None

                                        if sde_conn:
                                            sde_conn_path = os.path.join(local_path, sde_conn)
                                            if "FEATURE DATASET" in datasource_dict:
                                                sde_conn_path = os.path.join(sde_conn_path,
                                                                             datasource_dict["FEATURE DATASET"])
                                            fc_path = os.path.join(sde_conn_path, edit_dataset)

                                            split_dataset = edit_dataset.split('.')
                                            
                                            if "\\" in edit_dataset:
                                                schema = split_dataset[1][(split_dataset[1].find("\\") + 1):]
                                                fc_name = split_dataset[2]
                                            else:
                                                schema = split_dataset[0]
                                                fc_name = split_dataset[1]
                                            
                                            # Get FC Name
                                            try:
                                                while fc_name in cur_fc_relationships:
                                                    fc_name = f"{fc_name}_1"
                                                cur_fc_relationships[fc_name] = []
                                                temp_dict = cur_fc_relationships[fc_name]
                                                newConnectionProperties = {
                                                    'dataset': fc_name,
                                                    'workspace_factory': "File Geodatabase",
                                                    'connection_info': {'database': f'{gdb_path}'}}
                                                temp_dict.append(json.dumps(newConnectionProperties))
                                                temp_dict.append(str(cur_layer))
                                                temp_dict.append(json.dumps(connectionProperties))
                                                cur_fc_relationships[fc_name] = temp_dict
                                            
                                            except Exception:
                                                throw_exception(cur_logFile, str(cur_layer), currentLogFilePath)
                                            
                                            # Save FC to local GDB
                                            try:
                                                if fc_name != "GATES":  # TODO FIX GATES
                                                    arcpy.FeatureClassToFeatureClass_conversion(fc_path, gdb_path, fc_name,
                                                                                                where_clause=definition_query)
                                                    write_to_log(cur_logFile, f"{cur_layer} source changed to {fc_name} in {gdb_name}.")
                                            except Exception:
                                                throw_exception(cur_logFile, str(cur_layer), currentLogFilePath)
                                    
                                        else:
                                            write_to_log(cur_logFile,
                                                       f"{dataset} path unfound for {cur_layer}.")
                                    else:
                                        write_to_log(cur_logFile,
                                                   f"POTENTIAL ERROR: {cur_layer} has no valid 'user' connection info.")
                                else:
                                    write_to_log(cur_logFile,
                                               f"POTENTIAL ERROR: {cur_layer} has no valid connection info.")
                        else:
                            write_to_log(cur_logFile,
                                       f"{cur_layer} is a Basemap/Web Layer.")
                else:
                    write_to_log(cur_logFile, f"{cur_layer} is a Group Layer.")
        
        write_to_log(cur_logFile, f"[Process {increment}]: Complete", True)
        del replacement_prj
        cur_layer = ""
        return 0
    
    except Exception:
        throw_exception(cur_logFile, cur_layer, currentLogFilePath)
        return 1


# Update .aprx project layers to point to new datasets in local file geodatabase
def update_project(args):
    final_result = 0
    
    proc_num = args[0]
    fc_relationships = args[1]
    aprx_path = args[2]
    layers = args[3]
    logFile = args[4]
    proc_count = args[5]
    currentLogFilePath = args[6]
    index = 0
    threshold = len(layers) / proc_count
    lower_bounds = threshold * proc_num
    upper_bounds = lower_bounds + threshold
    
    try:
        # Update layer datasource in project
        for fc in fc_relationships.keys():
            if lower_bounds <= index < upper_bounds:
                cur_layer = fc_relationships[fc][1]
                old_connectionprop = json.loads(fc_relationships[fc][2])
                new_connectionprop = json.loads(fc_relationships[fc][0])
                
                lyr_index = -1
                for ind in range(0, len(layers)):
                    layer = layers[ind]
                    if layer == str(cur_layer):
                        lyr_index = ind
                
                if lyr_index > -1:
                    try:
                        if proc_count > 1:
                            lock.acquire()  # Only let one process open the ArcPro Project at a time
                        proj = arcpy.mp.ArcGISProject(aprx_path)
                        mapprj = proj.listMaps()[0]
                        aprx_layer_list = mapprj.listLayers()
                        replace_layer = aprx_layer_list[lyr_index]
                        connectionProperties = replace_layer.connectionProperties
                        if connectionProperties['connection_info']:
                            if 'user' in connectionProperties['connection_info']:
                                replace_layer.updateConnectionProperties(old_connectionprop, new_connectionprop)
                                replace_layer.definitionQuery = ""
                                if replace_layer.connectionProperties['dataset'] == new_connectionprop['dataset']:
                                    write_to_log(logFile,
                                               f"{replace_layer} successfully repointed to {new_connectionprop['dataset']}.")
                                else:
                                    write_to_log(logFile,
                                               f"POTENTIAL ERROR: {replace_layer} failed to repoint.", False)
                                    old_conn_props = replace_layer.connectionProperties['dataset']
                                    write_to_log(logFile,
                                               f"POTENTIAL ERROR: Old Connection Properties: {old_conn_props}.", False)
                                    write_to_log(logFile, f"POTENTIAL ERROR: New Connection Properties: {new_connectionprop['dataset']}.", False)
                            else:
                                write_to_log(logFile, f"POTENTIAL ERROR: {replace_layer} Skipped Repoint.", False)
                        else:
                            write_to_log(logFile, f"POTENTIAL ERROR: {replace_layer} Skipped Repoint.", False)
                            
                        proj.save()
                        del proj
                    
                    except Exception:
                        throw_exception(logFile, "", currentLogFilePath)
                        final_result = 1
                    
                    finally:
                        if proc_count > 1:
                            lock.release()
                
                else:
                    write_to_log(logFile, f"{cur_layer} is INVALID.", False, currentLogFilePath)
            
            index += 1
        
        write_to_log(logFile, f"[Process {proc_num}]: Complete", True)
        return final_result
    
    except Exception:
        throw_exception(logFile, "", currentLogFilePath)
        return 1


# Initialize lock for multiprocessing
def init_lock(l):
    global lock
    lock = l
    

def throw_exception(log_file: str, layer_name: str = "", temp_log_file=None):
    error = sys.exc_info()[0]
    if layer_name != "":
        write_to_log(log_file, f"ERROR with {layer_name}", True, temp_log_file)
    else:
        write_to_log(log_file, "ERROR", True, temp_log_file)
    write_to_log(log_file, str(error), False, temp_log_file)
    write_to_log(log_file, traceback.format_exc(), False, temp_log_file)
    
# Logging module for writing to text file
def write_to_log(logFile: str, message: str, timestamp: bool = False, temp_log_file: str = None):
    if timestamp is True:
        formatted_timestamp = f"{datetime.datetime.now().strftime('%m/%d/%y %I:%M:%S%p')}"
        with open(logFile, 'a') as log:
            log.write(f"{message} - {formatted_timestamp}.\n")
            print(f"{message} - {formatted_timestamp}.")
        if temp_log_file is not None:
            with open(temp_log_file, 'a') as tempLog:
                tempLog.write("{message} - {formatted_timestamp}.\n")
    else:
        with open(logFile, 'a') as log:
            log.write(f"{message}\n")
            print(f"{message}")
        if temp_log_file is not None:
            with open(temp_log_file, 'a') as tempLog:
                tempLog.write(f"{message}\n")


# Send Alert email on success/failure
def send_email(success: bool, log_path: str = "", log_list: list = None, service_name: str = ""):
    try:
        if success is True:
            subject = f"SUCCESS: {portal_name} | [{folder_name_global}]"
            message = f"Folder Path: {target_folder_path}\n\n"
            message += "Debug Log:\n"
            for log in log_list:
                message += "\n"
                with open(log, "r") as log_reader:
                    line = log_reader.readline()
        
                    while line:  # While Not Null
                        message += line
                        line = log_reader.readline()
                message += "\n"
                os.remove(log)
            email_recipients = "emailRecipientsSuccess.txt"
            
        else:
            subject = f"FAIL: {portal_name} | {service_name} [{folder_name_global}]"
            message = f"Folder Path: {target_folder_path}\n\n"
            message += "Debug Log:\n"
            with open(log_path, "r") as log:
                line = log.readline()
    
                while line:  # While Not Null
                    message += line
                    line = log.readline()
            os.remove(log_path)
            email_recipients = "emailRecipientsFail.txt"

        recipients = ""  # toList, get from File
        with open(email_recipients, "r") as emails:
            line = emails.readline()

            while line:  # While Not Null
                recipients += str(line).strip()
                recipients += ";"
                line = emails.readline()
            
        recipients = recipients[:len(recipients) - 1]
        parameters = {"pToList": recipients,
                      "pFromAddress": from_address,
                      "pSubject": subject,
                      "pCCList": "",
                      "pBCCList": "",
                      "pMessage": message}
        parsed_params = urllib.parse.urlencode(parameters)
        parsed_params = parsed_params.encode('ascii')
         
        response = urllib.request.urlopen("http://spuappweb/SPUemailService/SPUemailService.asmx/SendEmail",
                                          data=parsed_params)
            
    except Exception:
        throw_exception(log_file, "", cur_log_file_path)


# Produces delta in seconds between the first and last call of this function
def delta_time_system_timer(time_as_int):
    try:
        now = int(time.time())
        if time_as_int < 1:
            return now
        else:
            difference = now - time_as_int
            hours = int(difference / 3600)
            minutes = int((difference % 3600) / 60)
            seconds = int(difference % 60)
            return seconds, minutes, hours

    except Exception as e:
        return e


if __name__ == "__main__":
    main()
