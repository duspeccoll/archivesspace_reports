class ResourceAuditReport < AbstractReport

  register_report

  def fix_row(row)
    ReportUtils.fix_identifier_format(row, :call_number)
    ReportUtils.get_enum_values(row, [:extent_type])
    ReportUtils.fix_extent_format(row)
    add_sub_reports(row)
    row.delete(:resource_id)
  end

  def query_string
    "select
      id as resource_id,
      concat('/repositories/#{@repo_id}/resources/',id) as uri,
      title,
      identifier as call_number,
      inclusive_date,
      extent_number,
      extent_type
    from resource
      natural left outer join (
        select
          date.resource_id as id,
          date.expression as inclusive_date
        from date
          left join enumeration_value as date_label
            on date_label.id = date.label_id
          left join enumeration_value as date_type
            on date_type.id = date.date_type_id
        where date_label.value = 'creation'
          and date_type.value = 'inclusive'
      ) as inclusive_dates
      natural left outer join (
        select
          resource_id as id,
          number as extent_number,
          extent_type_id as extent_type
        from extent
      ) as extent_count
    where repo_id = #{db.literal(@repo_id)}
    order by call_number asc"
  end

  def add_sub_reports(row)
    id = row[:resource_id]
    row[:creators] = ResourceCreatorsSubreport.new(self, id).get_content
    row[:subjects] = ResourceTermsSubreport.new(self, id).get_content
    row[:abstract] = ResourceNotesSubreport.new(self, id, 'abstract').get_content
    row[:scopecontent] = ResourceNotesSubreport.new(self, id, 'scopecontent').get_content
    row[:bioghist] = ResourceNotesSubreport.new(self, id, 'bioghist').get_content
  end

  def identifier_field
    :call_number
  end

end
