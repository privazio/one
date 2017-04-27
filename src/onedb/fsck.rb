# -------------------------------------------------------------------------- #
# Copyright 2002-2016, OpenNebula Project, OpenNebula Systems                #
#                                                                            #
# Licensed under the Apache License, Version 2.0 (the "License"); you may    #
# not use this file except in compliance with the License. You may obtain    #
# a copy of the License at                                                   #
#                                                                            #
# http://www.apache.org/licenses/LICENSE-2.0                                 #
#                                                                            #
# Unless required by applicable law or agreed to in writing, software        #
# distributed under the License is distributed on an "AS IS" BASIS,          #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.   #
# See the License for the specific language governing permissions and        #
# limitations under the License.                                             #
#--------------------------------------------------------------------------- #

if !ONE_LOCATION
    LOG_LOCATION = "/var/log/one"
else
    LOG_LOCATION = ONE_LOCATION + "/var"
end

LOG              = LOG_LOCATION + "/onedb-fsck.log"

require "rexml/document"
include REXML
require 'ipaddr'
require 'set'

require 'nokogiri'

require 'opennebula'

require 'fsck/pool_control'
require 'fsck/user'
require 'fsck/group'

require 'fsck/cluster'
require 'fsck/host'
require 'fsck/datastore'
require 'fsck/network'

require 'fsck/image'
require 'fsck/marketplaceapp'
require 'fsck/marketplace'
require 'fsck/vm'
require 'fsck/cluster_vnc_bitmap'
require 'fsck/history'
require 'fsck/vrouter'

require 'fsck/user_quotas'
require 'fsck/group_quotas'

module OneDBFsck
    VERSION = "5.2.0"
    LOCAL_VERSION = "5.3.80"

    def db_version
        if defined?(@db_version) && @db_version
            @db_version
        else
            @db_version = read_db_version
        end
    end

    def check_db_version()
        # db_version = read_db_version()

        if ( db_version[:version] != VERSION ||
             db_version[:local_version] != LOCAL_VERSION )

            raise <<-EOT
Version mismatch: fsck file is for version
Shared: #{VERSION}, Local: #{LOCAL_VERSION}

Current database is version
Shared: #{db_version[:version]}, Local: #{db_version[:local_version]}
EOT
        end
    end

    def one_version
        "OpenNebula #{VERSION}"
    end

    # def db_version
    #     one_version()
    # end

    VM_BIN      = 0x0000001000000000
    NET_BIN     = 0x0000004000000000
    VROUTER_BIN = 0x0004000000000000
    HOLD        = 0xFFFFFFFF

    TABLES = ["group_pool", "user_pool", "acl", "image_pool", "host_pool",
        "network_pool", "template_pool", "vm_pool", "cluster_pool",
        "datastore_pool", "document_pool", "zone_pool", "secgroup_pool",
        "vdc_pool", "vrouter_pool", "marketplace_pool",
        "marketplaceapp_pool"].freeze

    FEDERATED_TABLES = ["group_pool", "user_pool", "acl", "zone_pool",
        "vdc_pool", "marketplace_pool", "marketplaceapp_pool"].freeze

    SCHEMA = {
        cluster_pool: "oid INTEGER PRIMARY KEY, name VARCHAR(128), " <<
            "body MEDIUMTEXT, uid INTEGER, gid INTEGER, owner_u INTEGER, " <<
            "group_u INTEGER, other_u INTEGER, UNIQUE(name)",
        cluster_datastore_relation: "cid INTEGER, oid INTEGER, " <<
            "PRIMARY KEY(cid, oid)",
        cluster_network_relation: "cid INTEGER, oid INTEGER, " <<
            "PRIMARY KEY(cid, oid)",
        datastore_pool: "oid INTEGER PRIMARY KEY, name VARCHAR(128), " <<
            "body MEDIUMTEXT, uid INTEGER, gid INTEGER, owner_u INTEGER, " <<
            "group_u INTEGER, other_u INTEGER",
        cluster_vnc_bitmap: "id INTEGER, map LONGTEXT, PRIMARY KEY(id)",
        host_pool: "oid INTEGER PRIMARY KEY, " <<
            "name VARCHAR(128), body MEDIUMTEXT, state INTEGER, " <<
            "last_mon_time INTEGER, uid INTEGER, gid INTEGER, " <<
            "owner_u INTEGER, group_u INTEGER, other_u INTEGER, " <<
            "cid INTEGER",
        image_pool: "oid INTEGER PRIMARY KEY, name VARCHAR(128), " <<
            "body MEDIUMTEXT, uid INTEGER, gid INTEGER, owner_u INTEGER, " <<
            "group_u INTEGER, other_u INTEGER, UNIQUE(name,uid)",
        network_pool: "oid INTEGER PRIMARY KEY, name VARCHAR(128), " <<
            "body MEDIUMTEXT, uid INTEGER, gid INTEGER, owner_u INTEGER, " <<
            "group_u INTEGER, other_u INTEGER, pid INTEGER, UNIQUE(name,uid)",
        user_quotas: "user_oid INTEGER PRIMARY KEY, body MEDIUMTEXT",
        group_quotas: "group_oid INTEGER PRIMARY KEY, body MEDIUMTEXT"
    }

    def tables
        TABLES
    end

    def federated_tables
        FEDERATED_TABLES
    end

    def create_table(type, name = nil)
        if name
            n = name.to_s
        else
            n = type.to_s
        end

        schema = SCHEMA[type]

        if !schema
            STDERR.puts "Schema not found (#{type})"
            exit(-1)
        end

        sql = "CREATE TABLE #{n} (#{schema});"

        STDERR.puts sql

        @db.run sql
    end

    def nokogiri_doc(body)
        Nokogiri::XML(body, nil, NOKOGIRI_ENCODING) do |c|
            c.default_xml.noblanks
        end
    end


    ########################################################################
    # Acl
    ########################################################################

    ########################################################################
    # Users
    #
    # USER/GNAME
    ########################################################################

    ########################################################################
    # Datastore
    #
    # DATASTORE/UID
    # DATASTORE/UNAME
    # DATASTORE/GID
    # DATASTORE/GNAME
    # DATASTORE/SYSTEM ??
    ########################################################################

    ########################################################################
    # VM Template
    #
    # VMTEMPLATE/UID
    # VMTEMPLATE/UNAME
    # VMTEMPLATE/GID
    # VMTEMPLATE/GNAME
    ########################################################################

    ########################################################################
    # Documents
    #
    # DOCUMENT/UID
    # DOCUMENT/UNAME
    # DOCUMENT/GID
    # DOCUMENT/GNAME
    ########################################################################

    ########################################################################
    # VM
    #
    # VM/UID
    # VM/UNAME
    # VM/GID
    # VM/GNAME
    #
    # VM/STATE        <--- Check transitioning states?
    # VM/LCM_STATE    <---- Check consistency state/lcm_state ?
    ########################################################################

    ########################################################################
    # Image
    #
    # IMAGE/UID
    # IMAGE/UNAME
    # IMAGE/GID
    # IMAGE/GNAME
    ########################################################################

    ########################################################################
    # VNet
    #
    # VNET/UID
    # VNET/UNAME
    # VNET/GID
    # VNET/GNAME
    ########################################################################


    def counters
        if !defined?(@counters)
            @counters = {}
            @counters[:host]  = {}
            @counters[:image] = {}
            @counters[:vnet]  = {}
            @counters[:vrouter]  = {}
        end

        @counters
    end

    # Initialize all the vrouter counters to 0
    def init_vrouter_counters
        @db.fetch("SELECT oid FROM vrouter_pool") do |row|
            counters[:vrouter][row[:oid]] = {
                :vms   => Set.new
            }
        end
    end

    def fsck
        init_log_time()

        @errors = 0
        @repaired_errors = 0
        @unrepaired_errors = 0

        puts

        db_version = read_db_version()

        ########################################################################
        # pool_control
        ########################################################################

        check_pool_control

        fix_pool_control

        log_time()

        ########################################################################
        # Groups
        #
        # GROUP/USERS/ID
        ########################################################################

        ########################################################################
        # Users
        #
        # USER/GID
        ########################################################################

        check_user
        fix_user

        log_time

        check_group
        fix_group

        log_time

        ########################################################################
        # Clusters
        #
        # CLUSTER/SYSTEM_DS
        # CLUSTER/HOSTS/ID
        # CLUSTER/DATASTORES/ID
        # CLUSTER/VNETS/ID
        ########################################################################
        # Datastore
        #
        # DATASTORE/CLUSTER_ID
        # DATASTORE/CLUSTER
        ########################################################################
        # VNet
        #
        # VNET/CLUSTER_ID
        # VNET/CLUSTER
        ########################################################################
        # Hosts
        #
        # HOST/CLUSTER_ID
        # HOST/CLUSTER
        ########################################################################

        init_cluster

        check_host_cluster
        fix_host_cluster

        log_time

        check_datastore_cluster
        fix_datastore_cluster

        log_time

        check_network_cluster
        fix_network_cluster

        log_time

        check_fix_cluster

        log_time

        check_fix_cluster_relations

        log_time

        ########################################################################
        # Datastore
        #
        # DATASTORE/IMAGES/ID
        ########################################################################
        # Image
        #
        # IMAGE/DATASTORE_ID
        # IMAGE/DATASTORE
        ########################################################################

        init_datastore_counters

        log_time

        check_datastore_image
        fix_datastore_image

        log_time

        check_fix_datastore

        log_time

        ########################################################################
        # VM Counters for host, image and vnet usage
        ########################################################################

        init_host_counters

        log_time

        init_image_counters

        log_time

        init_network_counters

        log_time

        init_vrouter_counters

        log_time

        check_vm
        fix_vm

        log_time

        # VNC

        # DATA: VNC Bitmap

        check_cluster_vnc_bitmap
        fix_cluster_vnc_bitmap

        log_time

        # history

        check_history
        fix_history

        log_time

        ########################################################################
        # Virtual Routers
        #
        # VROUTER/VMS/ID
        ########################################################################

        check_vrouter
        fix_vrouter

        log_time

        ########################################################################
        # DATA: Hosts
        #
        # HOST/HOST_SHARE/MEM_USAGE
        # HOST/HOST_SHARE/CPU_USAGE
        # HOST/HOST_SHARE/RUNNING_VMS
        # HOST/VMS/ID
        ########################################################################

        check_host
        fix_host

        log_time

        ########################################################################
        # DATA: Marketplace
        #
        # MARKETPLACE/MARKETPLACEAPPS/ID
        ########################################################################
        # DATA: App
        #
        # MARKETPLACEAPP/MARKETPLACE_ID
        # MARKETPLACEAPP/MARKETPLACE
        # MARKETPLACEAPP/ORIGIN_ID
        ########################################################################

        check_marketplaceapp

        fix_marketplaceapp

        log_time()

        check_marketplace

        fix_marketplace

        log_time()

        ########################################################################
        # DATA: Image
        #
        # IMAGE/RUNNING_VMS
        # IMAGE/VMS/ID
        #
        # IMAGE/CLONING_OPS
        # IMAGE/CLONES/ID
        # IMAGE/APP_CLONES/ID
        #
        # IMAGE/CLONING_ID
        #
        # IMAGE/STATE
        ########################################################################

        check_image

        fix_image

        log_time

        ########################################################################
        # VNet
        #
        # LEASES
        ########################################################################

        init_network_lease_counters

        check_network
        fix_network

        log_time

        ########################################################################
        # Users
        #
        # USER QUOTAS
        ########################################################################

        check_fix_user_quotas

        log_time

        ########################################################################
        # Groups
        #
        # GROUP QUOTAS
        ########################################################################

        check_fix_group_quotas

        log_time

        ########################################################################
        # VM Templates
        #
        # TEMPLATE/OS/BOOT
        ########################################################################

        templates_fix = {}

        @db.transaction do
        @db[:template_pool].each do |row|
            doc = Nokogiri::XML(row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

            boot = doc.root.at_xpath("TEMPLATE/OS/BOOT")

            if boot.nil? || boot.text.downcase.match(/fd|hd|cdrom|network/).nil?
              next
            end

            # Note: this code assumes that disks are ordered in the same order as
            # their target, and may break boot order if the target is not left
            # completely to oned.
            # If, for example, the third disk ends with target="vda",
            # boot="hd" should be updated to boot="disk2", but it is not

            devs = []

            hd_i      = 0
            cdrom_i   = 0
            network_i = 0

            error = false

            boot.text.split(",").each do |dev|
                dev.downcase!

                case dev
                when "hd", "cdrom"
                    index = nil
                    if dev == "hd"
                        index = hd_i
                        hd_i += 1
                    else
                        index = cdrom_i
                        cdrom_i += 1
                    end

                    id = get_disk_id(dev, index, doc)
                    if id.nil?
                        log_error("VM Template #{row[:oid]} OS/BOOT contains deprecated format \"#{boot.content}\", but DISK ##{index} of type #{dev} could not be found to fix it automatically", false)
                        error = true
                    end
                    devs.push("disk#{id}")

                when "network"
                    devs.push("nic#{network_i}")
                    network_i += 1

                when "fd"
                    log_error("VM Template #{row[:oid]} OS/BOOT contains deprecated format \"#{boot.content}\", but \"fd\" is not supported anymore and can't be fixed automatically", false)
                    error = true

                else
                    log_error("VM Template #{row[:oid]} OS/BOOT contains deprecated format \"#{boot.content}\", but it can't be parsed to be fixed automatically", false)
                    error = true

                end
            end

            if error
                next
            end

            new_boot = devs.join(",")

            log_error("VM Template #{row[:oid]} OS/BOOT contains deprecated format \"#{boot.content}\", is was updated to #{new_boot}")

            boot.content = new_boot

            templates_fix[row[:oid]] = doc.root.to_s
        end
        end


        @db.transaction do
            templates_fix.each do |id, body|
                @db[:template_pool].where(:oid => id).update(:body => body)
            end
        end

        log_time()

        log_total_errors()

        return true
    end

    def log_error(message, repaired=true)
        @errors += 1

        if repaired
            @repaired_errors += 1
        else
            @unrepaired_errors += 1
        end

        if !repaired
            message = "[UNREPAIRED] " + message
        end

        log_msg(message)
    end

    def log_msg(message)
        @log_file ||= File.open(LOG, "w")

        puts message

        @log_file.puts(message)
        @log_file.flush
    end

    def log_total_errors()
        puts
        log_msg "Total errors found: #{@errors}"
        log_msg "Total errors repaired: #{@repaired_errors}"
        log_msg "Total errors unrepaired: #{@unrepaired_errors}"

        puts "A copy of this output was stored in #{LOG}"
    end



    def calculate_quotas(doc, where_filter, resource)

        oid = doc.root.at_xpath("ID").text.to_i

        # VM quotas
        cpu_used = 0
        mem_used = 0
        vms_used = 0
        sys_used = 0

        # VNet quotas
        vnet_usage = {}

        # Image quotas
        img_usage = {}

        @db.fetch("SELECT body FROM vm_pool WHERE #{where_filter} AND state<>6") do |vm_row|
            vmdoc = Nokogiri::XML(vm_row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

            # VM quotas
            vmdoc.root.xpath("TEMPLATE/CPU").each { |e|
                # truncate to 2 decimals
                cpu = (e.text.to_f * 100).to_i
                cpu_used += cpu
            }

            vmdoc.root.xpath("TEMPLATE/MEMORY").each { |e|
                mem_used += e.text.to_i
            }

            vmdoc.root.xpath("TEMPLATE/DISK").each { |e|
                type = ""

                e.xpath("TYPE").each { |t_elem|
                    type = t_elem.text.upcase
                }

                size = 0

                if !e.at_xpath("SIZE").nil?
                    size = e.at_xpath("SIZE").text.to_i
                end

                if ( type == "SWAP" || type == "FS")
                    sys_used += size
                else
                    if !e.at_xpath("CLONE").nil?
                        clone = (e.at_xpath("CLONE").text.upcase == "YES")

                        target = nil

                        if clone
                            target = e.at_xpath("CLONE_TARGET").text if !e.at_xpath("CLONE_TARGET").nil?
                        else
                            target = e.at_xpath("LN_TARGET").text if !e.at_xpath("LN_TARGET").nil?
                        end

                        if !target.nil? && target == "SYSTEM"
                            sys_used += size

                            if !e.at_xpath("DISK_SNAPSHOT_TOTAL_SIZE").nil?
                                sys_used += e.at_xpath("DISK_SNAPSHOT_TOTAL_SIZE").text.to_i
                            end
                        end
                    end
                end
            }

            vms_used += 1

            # VNet quotas
            vmdoc.root.xpath("TEMPLATE/NIC/NETWORK_ID").each { |e|
                vnet_usage[e.text] = 0 if vnet_usage[e.text].nil?
                vnet_usage[e.text] += 1
            }

            # Image quotas
            vmdoc.root.xpath("TEMPLATE/DISK/IMAGE_ID").each { |e|
                img_usage[e.text] = 0 if img_usage[e.text].nil?
                img_usage[e.text] += 1
            }
        end


        @db.fetch("SELECT body FROM vrouter_pool WHERE #{where_filter}") do |vrouter_row|
            vrouter_doc = Nokogiri::XML(vrouter_row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

            # VNet quotas
            vrouter_doc.root.xpath("TEMPLATE/NIC").each { |nic|
                net_id = nil
                nic.xpath("NETWORK_ID").each do |nid|
                    net_id = nid.text
                end

                floating = false

                nic.xpath("FLOATING_IP").each do |floating_e|
                    floating = (floating_e.text.upcase == "YES")
                end

                if !net_id.nil? && floating
                    vnet_usage[net_id] = 0 if vnet_usage[net_id].nil?

                    vnet_usage[net_id] += 1
                end
            }

        end

        # VM quotas

        vm_elem = nil
        doc.root.xpath("VM_QUOTA/VM").each { |e| vm_elem = e }

        if vm_elem.nil?
            doc.root.xpath("VM_QUOTA").each { |e| e.remove }

            vm_quota  = doc.root.add_child(doc.create_element("VM_QUOTA"))
            vm_elem   = vm_quota.add_child(doc.create_element("VM"))

            vm_elem.add_child(doc.create_element("CPU")).content         = "-1"
            vm_elem.add_child(doc.create_element("CPU_USED")).content    = "0"

            vm_elem.add_child(doc.create_element("MEMORY")).content      = "-1"
            vm_elem.add_child(doc.create_element("MEMORY_USED")).content = "0"

            vm_elem.add_child(doc.create_element("VMS")).content         = "-1"
            vm_elem.add_child(doc.create_element("VMS_USED")).content    = "0"

            vm_elem.add_child(doc.create_element("SYSTEM_DISK_SIZE")).content       = "-1"
            vm_elem.add_child(doc.create_element("SYSTEM_DISK_SIZE_USED")).content  = "0"
        end


        vm_elem.xpath("CPU_USED").each { |e|

            # Because of bug http://dev.opennebula.org/issues/1567 the element
            # may contain a float number in scientific notation.

            # Check if the float value or the string representation mismatch,
            # but ignoring the precision

            cpu_used = (cpu_used / 100.0)

            different = ( e.text.to_f != cpu_used ||
                ![sprintf('%.2f', cpu_used), sprintf('%.1f', cpu_used), sprintf('%.0f', cpu_used)].include?(e.text)  )

            cpu_used_str = sprintf('%.2f', cpu_used)

            if different
                log_error("#{resource} #{oid} quotas: CPU_USED has #{e.text} \tis\t#{cpu_used_str}")
                e.content = cpu_used_str
            end
        }

        vm_elem.xpath("MEMORY_USED").each { |e|
            if e.text != mem_used.to_s
                log_error("#{resource} #{oid} quotas: MEMORY_USED has #{e.text} \tis\t#{mem_used}")
                e.content = mem_used.to_s
            end
        }

        vm_elem.xpath("VMS_USED").each { |e|
            if e.text != vms_used.to_s
                log_error("#{resource} #{oid} quotas: VMS_USED has #{e.text} \tis\t#{vms_used}")
                e.content = vms_used.to_s
            end
        }

        vm_elem.xpath("SYSTEM_DISK_SIZE_USED").each { |e|
            if e.text != sys_used.to_s
                log_error("#{resource} #{oid} quotas: SYSTEM_DISK_SIZE_USED has #{e.text} \tis\t#{sys_used}")
                e.content = sys_used.to_s
            end
        }

        # VNet quotas

        net_quota = nil
        doc.root.xpath("NETWORK_QUOTA").each { |e| net_quota = e }

        if net_quota.nil?
            net_quota = doc.root.add_child(doc.create_element("NETWORK_QUOTA"))
        end

        net_quota.xpath("NETWORK").each { |net_elem|
            vnet_id = net_elem.at_xpath("ID").text

            leases_used = vnet_usage.delete(vnet_id)

            leases_used = 0 if leases_used.nil?

            net_elem.xpath("LEASES_USED").each { |e|
                if e.text != leases_used.to_s
                    log_error("#{resource} #{oid} quotas: VNet #{vnet_id}\tLEASES_USED has #{e.text} \tis\t#{leases_used}")
                    e.content = leases_used.to_s
                end
            }
        }

        vnet_usage.each { |vnet_id, leases_used|
            log_error("#{resource} #{oid} quotas: VNet #{vnet_id}\tLEASES_USED has 0 \tis\t#{leases_used}")

            new_elem = net_quota.add_child(doc.create_element("NETWORK"))

            new_elem.add_child(doc.create_element("ID")).content = vnet_id
            new_elem.add_child(doc.create_element("LEASES")).content = "-1"
            new_elem.add_child(doc.create_element("LEASES_USED")).content = leases_used.to_s
        }


        # Image quotas

        img_quota = nil
        doc.root.xpath("IMAGE_QUOTA").each { |e| img_quota = e }

        if img_quota.nil?
            img_quota = doc.root.add_child(doc.create_element("IMAGE_QUOTA"))
        end

        img_quota.xpath("IMAGE").each { |img_elem|
            img_id = img_elem.at_xpath("ID").text

            rvms = img_usage.delete(img_id)

            rvms = 0 if rvms.nil?

            img_elem.xpath("RVMS_USED").each { |e|
                if e.text != rvms.to_s
                    log_error("#{resource} #{oid} quotas: Image #{img_id}\tRVMS has #{e.text} \tis\t#{rvms}")
                    e.content = rvms.to_s
                end
            }
        }

        img_usage.each { |img_id, rvms|
            log_error("#{resource} #{oid} quotas: Image #{img_id}\tRVMS has 0 \tis\t#{rvms}")

            new_elem = img_quota.add_child(doc.create_element("IMAGE"))

            new_elem.add_child(doc.create_element("ID")).content = img_id
            new_elem.add_child(doc.create_element("RVMS")).content = "-1"
            new_elem.add_child(doc.create_element("RVMS_USED")).content = rvms.to_s
        }

        # Datastore quotas
        ds_usage = {}

        @db.fetch("SELECT body FROM image_pool WHERE #{where_filter}") do |img_row|
            img_doc = Nokogiri::XML(img_row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

            img_doc.root.xpath("DATASTORE_ID").each { |e|
                ds_usage[e.text] = [0,0] if ds_usage[e.text].nil?
                ds_usage[e.text][0] += 1

                img_doc.root.xpath("SIZE").each { |size|
                    ds_usage[e.text][1] += size.text.to_i
                }

                img_doc.root.xpath("SNAPSHOTS/SNAPSHOT/SIZE").each { |size|
                    ds_usage[e.text][1] += size.text.to_i
                }
            }
        end

        ds_quota = nil
        doc.root.xpath("DATASTORE_QUOTA").each { |e| ds_quota = e }

        if ds_quota.nil?
            ds_quota = doc.root.add_child(doc.create_element("DATASTORE_QUOTA"))
        end

        ds_quota.xpath("DATASTORE").each { |ds_elem|
            ds_id = ds_elem.at_xpath("ID").text

            images_used,size_used = ds_usage.delete(ds_id)

            images_used = 0 if images_used.nil?
            size_used   = 0 if size_used.nil?

            ds_elem.xpath("IMAGES_USED").each { |e|
                if e.text != images_used.to_s
                    log_error("#{resource} #{oid} quotas: Datastore #{ds_id}\tIMAGES_USED has #{e.text} \tis\t#{images_used}")
                    e.content = images_used.to_s
                end
            }

            ds_elem.xpath("SIZE_USED").each { |e|
                if e.text != size_used.to_s
                    log_error("#{resource} #{oid} quotas: Datastore #{ds_id}\tSIZE_USED has #{e.text} \tis\t#{size_used}")
                    e.content = size_used.to_s
                end
            }
        }

        ds_usage.each { |ds_id, array|
            images_used,size_used = array

            log_error("#{resource} #{oid} quotas: Datastore #{ds_id}\tIMAGES_USED has 0 \tis\t#{images_used}")
            log_error("#{resource} #{oid} quotas: Datastore #{ds_id}\tSIZE_USED has 0 \tis\t#{size_used}")

            new_elem = ds_quota.add_child(doc.create_element("DATASTORE"))

            new_elem.add_child(doc.create_element("ID")).content = ds_id

            new_elem.add_child(doc.create_element("IMAGES")).content = "-1"
            new_elem.add_child(doc.create_element("IMAGES_USED")).content = images_used.to_s

            new_elem.add_child(doc.create_element("SIZE")).content = "-1"
            new_elem.add_child(doc.create_element("SIZE_USED")).content = size_used.to_s
        }
    end

    def mac_s_to_i(mac)
        return nil if mac.empty?
        return mac.split(":").map {|e|
            e.to_i(16).to_s(16).rjust(2,"0")}.join("").to_i(16)
    end

    def mac_i_to_s(mac)
        mac_s = mac.to_s(16).rjust(12, "0")
        return "#{mac_s[0..1]}:#{mac_s[2..3]}:#{mac_s[4..5]}:"<<
               "#{mac_s[6..7]}:#{mac_s[8..9]}:#{mac_s[10..11]}"
    end

    def ip6_prefix_s_to_i(prefix)
        return nil if prefix.empty?
        return prefix.split(":", 4).map {|e|
            e.to_i(16).to_s(16).rjust(4, "0")}.join("").to_i(16)
    end

    # Copied from AddressRange::set_ip6 in AddressRange.cc
    def mac_to_ip6_suffix(mac_i)
        mac = [
            mac_i & 0x00000000FFFFFFFF,
            (mac_i & 0xFFFFFFFF00000000) >> 32
        ]

        mlow = mac[0]
        eui64 = [
            4261412864 + (mlow & 0x00FFFFFF),
            ((mac[1]+512)<<16) + ((mlow & 0xFF000000)>>16) + 0x000000FF
        ]

        return (eui64[1] << 32) + eui64[0]
    end

    def lease_to_s(lease)
        return lease[:ip].nil? ? lease[:mac].to_s : lease[:ip].to_s
    end

    # Returns the ID of the # disk of a type
    # Params:
    # +type+:: type name of the disk, can be “hd” or “cdrom”
    # +doc+:: Nokogiri::XML::Node describing the VM template
    def get_disk_id(type, index, doc)
        found_i = -1

        doc.root.xpath("TEMPLATE/DISK").each_with_index do |disk, disk_i|
            id = disk.at_xpath("IMAGE_ID")
            if ! id.nil?
                image = get_image_from_id(id.content)
            else
                image = get_image_from_name(disk)
            end

            next if image.nil?

            if is_image_type_matching?(image, type)
                found_i += 1

                if (found_i == index)
                    return disk_i
                end
            end
        end

        return nil
    end

    # Returns a Nokogiri::XML::Node describing an image
    # Params:
    # +id+:: ID of the image
    def get_image_from_id(id)
        row = @db.fetch("SELECT body from image_pool where oid=#{id}").first
        # No image found, so unable to get image TYPE
        return nil if row.nil?

        image = Nokogiri::XML(row[:body], nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}
        return image
    end

    # Returns a Nokogiri::XML::Node describing an image
    # Params:
    # +disk+:: Nokogiri::XML::Node describing a disk used by a template
    def get_image_from_name(disk)
      name = disk.at_xpath("IMAGE") && disk.at_xpath("IMAGE").content
      uid = disk.at_xpath("IMAGE_UID")
      uname = disk.at_xpath("IMAGE_UNAME")

      if ! name.nil? and (! uid.nil? or ! uname.nil?)
        if uid.nil?
          uid = get_user_id(uname.content)
        else
          uid = uid.content
        end

        return nil if uid.nil?

        row = @db.fetch("SELECT body from image_pool where name=\"#{name}\" and uid=#{uid}").first
        # No image found, so unable to get image TYPE
        return nil if row.nil?

        image = Nokogiri::XML(row[:body], nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}
        return image
      end

      return nil
    end

    # Returns the ID of a user name
    # Params:
    # +name+:: name of a user
    def get_user_id(name)
        row = @db.fetch("SELECT uid from user_pool WHERE name=\"#{name}\"").first

        return nil if row.nil?

        return row[:uid]
    end

    # Check if an image type match the type used in template BOOT
    # Params:
    # +image_type+:: doc
    # +wanted_type+:: string type extracted from VM template BOOT
    def is_image_type_matching?(image, wanted_type)
        return false if image.nil? || image.at_xpath("IMAGE/TYPE").nil?

        img_type = OpenNebula::Image::IMAGE_TYPES[image.at_xpath("IMAGE/TYPE").text.to_i]

        if wanted_type == "hd"
            return img_type == "OS" || img_type == "DATABLOCK"
        else
            return img_type == "CDROM"
        end
    end
end
