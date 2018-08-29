class ResourceProcessingNotesReport < AbstractReport

  register_report

  def fix_row(row)
    clean_row(row)
  end

  def query_string
    "(select
      identifier,
      'Resource' as type,
      title,
      repository_processing_note as processing_note
    from resource
    where repo_id = #{db.literal(@repo_id)}
    and repository_processing_note is not null)

    union

    (select
      component_id as identifier,
      'Archival Object' as type,
      title,
      repository_processing_note as processing_note
    from archival_object
    where repo_id = #{db.literal(@repo_id)}
    and repository_processing_note is not null)"
  end

  def clean_row(row)
    ReportUtils.fix_identifier_format(row, :identifier) if row[:type] == 'Resource'
  end

end
