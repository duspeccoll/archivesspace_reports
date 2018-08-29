class SubjectListReport < AbstractReport

  register_report

  def query_string
    "select
      concat('/subjects/', subject.id) as uri,
      subject.title as heading,
      subject.source_id as source,
      subject.authority_id as authority,
      subject.scope_note as scope_note,
      first_term_type.term_type_id as term_type,
      linked_records.count as linked_records
    from subject
      left outer join (
        select
          subject.id as id,
          term.id as term_type_id
        from subject
          left outer join subject_term
            on subject_term.subject_id = subject.id
          left outer join term
            on term.id = subject_term.term_id
        where term.term = substring_index(subject.title,' -- ', 1)
      ) as first_term_type
        on subject.id = first_term_type.id
      left outer join (
        select
          subject.id as id,
          count(subject_rlshp.id) as count
        from subject
          left outer join subject_rlshp
            on subject_rlshp.subject_id = subject.id
        group by subject.id
      ) as linked_records
        on subject.id = linked_records.id
    order by heading asc"
  end

  def fix_row(row)
    ReportUtils.get_enum_values(row, [:source, :term_type])
  end

  def page_break
    false
  end
end
