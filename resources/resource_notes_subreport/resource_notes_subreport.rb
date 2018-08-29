class ResourceNotesSubreport < AbstractSubreport

  register_subreport('notes', ['resource'])

  def initialize(parent_report, resource_id, note_type)
    super(parent_report)

    @resource_id = resource_id
    @note_type = note_type
  end

  def query_string
    "select notes from note
    where resource_id = #{db.literal(@resource_id)}
      and notes like '%\"type\":\"#{@note_type}\"%'"
  end

  # notes are JSON strings so we gotta parse 'em
  def fix_row(row)
    note = row[:notes]
    if ASUtils.blank?(note)
      row[:note] = note
    else
      json = ASUtils.json_parse(note)
      if json['jsonmodel_type'] == "note_singlepart"
        row[:note] = json['content'].compact.join(' ').gsub(/<(\/)?p>/,'')
      elsif json['jsonmodel_type'] == "note_multipart"
        notes = []
        json['subnotes'].each {|subnote| notes.push(subnote['content'])}
        row[:note] = notes.compact.join(' ').gsub(/<(\/)?p>/,'')
      end
    end
    row.delete(:notes)
  end

  def self.field_name
    "#{@note_type}"
  end

end
