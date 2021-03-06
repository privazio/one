# -------------------------------------------------------------------------- #
# Copyright 2002-2017, OpenNebula Project, OpenNebula Systems                #
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


require 'set'
require 'base64'
require 'zlib'
require 'pathname'
require 'yaml'
require 'opennebula'

$: << File.dirname(__FILE__)

include OpenNebula

module Migrator
    def db_version
        "5.3.80"
    end

    def one_version
        "OpenNebula 5.3.80"
    end

    def up
        init_log_time()

        feature_5136()

        feature_4901()

        feature_5005()

        feature_2347()

        bug_3705()

        feature_4809()
        log_time()

        return true
    end

    private

    def xpath(doc, sxpath)
        element = doc.root.at_xpath(sxpath)
        if !element.nil?
            element.text
        else
            ""
        end
    end

    ############################################################################
    # Feature 5136. Improve ec2 keys_ids_security
    #
    ############################################################################
    def feature_5136
        ec2_driver_conf = "#{ETC_LOCATION}/ec2_driver.conf"
        token = File.read(VAR_LOCATION+'/.one/one_key')
        opts = {}

        begin
            ec2_conf = YAML::load(File.read(ec2_driver_conf))
        rescue Exception => e
            str_error="ec2_driver.conf invalid syntax!"
            raise str_error
        end

        regions = ec2_conf["regions"]
        @db.run "ALTER TABLE host_pool RENAME TO old_host_pool;"
        create_table(:host_pool)

        @db.transaction do
            @db.fetch("SELECT * FROM old_host_pool") do |row|
                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                    c.default_xml.noblanks
                }
                template = doc.root.at_xpath("TEMPLATE")

                if xpath(doc, "TEMPLATE/HYPERVISOR").to_s == "ec2"

                    host_name = xpath(doc, "NAME").to_s
                    host_info = ( regions[host_name].nil? ? regions["default"] : regions[host_name] )

                    opts["EC2_ACCESS"]=host_info["access_key_id"]
                    opts["EC2_SECRET"]=host_info["secret_access_key"]

                    OpenNebula.encrypt(opts, token).each { |k, v|
                        template.add_child(doc.create_element k, v)
                    }
                end

                row[:body] = doc.root.to_s
                @db[:host_pool].insert(row)
            end
        end

        @db.run "DROP TABLE old_host_pool;"
    end

    ############################################################################
    # Feature 4921. Adds TOTAL_CPU and TOTAL_MEM to HOST/HOST_SHARE to compute
    # MAX_CPU and MAX_MEM when RESERVED_CPU/MEM is updated
    ############################################################################
    def feature_4901
        @db.run "ALTER TABLE host_pool RENAME TO old_host_pool;"
        create_table(:host_pool)

        @db.transaction do
            @db.fetch("SELECT * FROM old_host_pool") do |row|
                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                    c.default_xml.noblanks
                }

                rcpu = xpath(doc, "TEMPLATE/RESERVED_CPU").to_i
                rmem = xpath(doc, "TEMPLATE/RESERVED_MEM").to_i

                total_cpu = xpath(doc, "HOST_SHARE/MAX_CPU").to_i + rcpu
                total_mem = xpath(doc, "HOST_SHARE/MAX_MEM").to_i + rmem

                total_cpu_e = doc.create_element "TOTAL_CPU", total_cpu
                total_mem_e = doc.create_element "TOTAL_MEM", total_mem

                host_share = doc.root.at_xpath("HOST_SHARE")
                host_share.add_child(total_cpu_e)
                host_share.add_child(total_mem_e)

                row[:body] = doc.root.to_s

                @db[:host_pool].insert(row)
            end
        end

        @db.run "DROP TABLE old_host_pool;"
    end

    ############################################################################
    # Feature 5005.
    # Adds UID, GID and REQUEST_ID to history records
    ############################################################################
    def feature_5005
        @db.run "ALTER TABLE vm_pool RENAME TO old_vm_pool;"
        create_table(:vm_pool)

        @db.transaction do
            @db.fetch("SELECT * FROM old_vm_pool") do |row|

                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                  c.default_xml.noblanks
                }

                doc.root.xpath("HISTORY_RECORDS/HISTORY").each do |h|
                    reason = h.xpath("REASON")
                    reason.unlink if !reason.nil?

                    uid = doc.create_element "UID", -1
                    gid = doc.create_element "GID", -1
                    rid = doc.create_element "REQUEST_ID", -1

                    h.add_child(uid)
                    h.add_child(gid)
                    h.add_child(rid)
                end

                row[:body] = doc.root.to_s

                @db[:vm_pool].insert(row)
            end
        end

        @db.run "DROP TABLE old_vm_pool;"

        @db.run "ALTER TABLE history RENAME TO old_history;"
        create_table(:history)

        @db.transaction do
            @db.fetch("SELECT * FROM old_history") do |row|
                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                    c.default_xml.noblanks
                }

                h = doc.root

                reason = h.xpath("REASON")
                reason.unlink if !reason.nil?

                uid = doc.create_element "UID", -1
                gid = doc.create_element "GID", -1
                rid = doc.create_element "REQUEST_ID", -1

                h.add_child(uid)
                h.add_child(gid)
                h.add_child(rid)

                row[:body] = doc.root.to_s

                @db[:history].insert(row)
            end
        end

        @db.run "DROP TABLE old_history;"
    end

    def feature_2347
        create_table(:vmgroup_pool)
    end

    ############################################################################
    # Bug 3705
    # Adds DRIVER to CEPH and LVM image datastores
    ############################################################################
    def bug_3705
        @db.run "ALTER TABLE datastore_pool RENAME TO old_datastore_pool;"
        create_table(:datastore_pool)

        @db.transaction do
            @db.fetch("SELECT * FROM old_datastore_pool") do |row|
                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                    c.default_xml.noblanks
                }

                type = xpath(doc, 'TYPE').to_i
                tm_mad = xpath(doc, 'TM_MAD')

                if (type == 0) && (["ceph", "fs_lvm"].include?(tm_mad))
                    doc.root.xpath("TEMPLATE/DRIVER").each do |d|
                        d.remove
                    end

                    driver = doc.create_element "DRIVER", "raw"
                    doc.root.at_xpath("TEMPLATE").add_child(driver)

                    row[:body] = doc.root.to_s
                end

                @db[:datastore_pool].insert(row)
            end
        end

        @db.run "DROP TABLE old_datastore_pool;"
    end

    ############################################################################
    # Feature 4809
    # Simplify HA management in OpenNebula
    ############################################################################
    def feature_4809
        create_table(:logdb)
        create_table(:fed_logdb)

        @db.run "ALTER TABLE zone_pool RENAME TO old_zone_pool;"
        create_table(:zone_pool)

        @db.transaction do
            @db.fetch("SELECT * FROM old_zone_pool") do |row|
                doc = Nokogiri::XML(row[:body], nil, NOKOGIRI_ENCODING) { |c|
                    c.default_xml.noblanks
                }

                zedp = xpath(doc, "TEMPLATE/ENDPOINT")

                server_pool = doc.create_element "SERVER_POOL"
                server      = doc.create_element "SERVER"

                id   = doc.create_element "ID", 0
                name = doc.create_element "NAME", "zone_server"
                edp  = doc.create_element "ENDPOINT", zedp

                server.add_child(id)
                server.add_child(name)
                server.add_child(edp)

                server_pool.add_child(server)

                doc.root.add_child(server_pool)

                row[:body] = doc.root.to_s

                @db[:zone_pool].insert(row)
            end
        end

        @db.run "DROP TABLE old_zone_pool;"

    end
end
