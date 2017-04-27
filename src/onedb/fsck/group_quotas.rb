
module OneDBFsck
    def check_fix_group_quotas
        # This block is not needed for now
=begin
        @db.transaction do
            @db.fetch("SELECT oid FROM group_pool") do |row|
                found = false

                @db.fetch("SELECT group_oid FROM group_quotas WHERE group_oid=#{row[:oid]}") do |q_row|
                    found = true
                end

                if !found
                    log_error("Group #{row[:oid]} does not have a quotas entry")

                    @db.run "INSERT INTO group_quotas VALUES(#{row[:oid]},'<QUOTAS><ID>#{row[:oid]}</ID><DATASTORE_QUOTA></DATASTORE_QUOTA><NETWORK_QUOTA></NETWORK_QUOTA><VM_QUOTA></VM_QUOTA><IMAGE_QUOTA></IMAGE_QUOTA></QUOTAS>');"
                end
            end
        end
=end
        @db.run "ALTER TABLE group_quotas RENAME TO old_group_quotas;"
        create_table(:group_quotas)

        @db.transaction do
            # oneadmin does not have quotas
            @db.fetch("SELECT * FROM old_group_quotas WHERE group_oid=0") do |row|
                @db[:group_quotas].insert(row)
            end

            @db.fetch("SELECT * FROM old_group_quotas WHERE group_oid>0") do |row|
                doc = Nokogiri::XML(row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

                calculate_quotas(doc, "gid=#{row[:group_oid]}", "Group")

                @db[:group_quotas].insert(
                    :group_oid   => row[:group_oid],
                    :body       => doc.root.to_s)
            end
        end

        @db.run "DROP TABLE old_group_quotas;"
    end
end

