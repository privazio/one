
module OneDBFsck
    def check_fix_user_quotas
        # This block is not needed for now
=begin
        @db.transaction do
            @db.fetch("SELECT oid FROM user_pool") do |row|
                found = false

                @db.fetch("SELECT user_oid FROM user_quotas WHERE user_oid=#{row[:oid]}") do |q_row|
                    found = true
                end

                if !found
                    log_error("User #{row[:oid]} does not have a quotas entry")

                    @db.run "INSERT INTO user_quotas VALUES(#{row[:oid]},'<QUOTAS><ID>#{row[:oid]}</ID><DATASTORE_QUOTA></DATASTORE_QUOTA><NETWORK_QUOTA></NETWORK_QUOTA><VM_QUOTA></VM_QUOTA><IMAGE_QUOTA></IMAGE_QUOTA></QUOTAS>');"
                end
            end
        end
=end
        @db.run "ALTER TABLE user_quotas RENAME TO old_user_quotas;"
        create_table(:user_quotas)

        @db.transaction do
            # oneadmin does not have quotas
            @db.fetch("SELECT * FROM old_user_quotas WHERE user_oid=0") do |row|
                @db[:user_quotas].insert(row)
            end

            @db.fetch("SELECT * FROM old_user_quotas WHERE user_oid>0") do |row|
                doc = Nokogiri::XML(row[:body],nil,NOKOGIRI_ENCODING){|c| c.default_xml.noblanks}

                calculate_quotas(doc, "uid=#{row[:user_oid]}", "User")

                @db[:user_quotas].insert(
                    :user_oid   => row[:user_oid],
                    :body       => doc.root.to_s)
            end
        end

        @db.run "DROP TABLE old_user_quotas;"
    end
end

