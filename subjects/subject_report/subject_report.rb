class SubjectReport < AbstractReport

  register_report

  def headers
    ['heading', 'source', 'authority_id', 'scope_note', 'first_term_type', 'linked_records']
  end

  def query
    first_term_types = db[:subject]
      .left_outer_join(:subject_term, :subject_id => :subject__id)
      .left_outer_join(:term, :id => :subject_term__term_id)
      .left_outer_join(:enumeration_value, {:id => :term__term_type_id}, :table_alias => :term_type)
      .select(Sequel.as(:subject__id, :id),
        Sequel.as(:term_type__value, :value))
      .where(:term__term => Sequel.lit("substring_index(subject.title,' -- ',1)"))

    linked_records = db[:subject]
      .left_outer_join(:subject_rlshp, :subject_id => :subject__id)
      .select(Sequel.as(:subject__id, :id))
      .select_more{Sequel.as(count(:subject_rlshp__id), :linked_records)}
      .group(:subject__id)

    db[:subject]
      .left_outer_join(:enumeration_value, {:id => :subject__source_id}, :table_alias => :source)
      .left_outer_join(first_term_types, {:id => :subject__id}, :table_alias => :first_term_type)
      .left_outer_join(linked_records, {:id => :subject__id}, :table_alias => :linked_record)
      .select(Sequel.as(:subject__title, :heading),
        Sequel.as(:source__value, :source),
        Sequel.as(:subject__authority_id, :authority_id),
        Sequel.as(:subject__scope_note, :scope_note),
        Sequel.as(:first_term_type__value, :first_term_type),
        Sequel.as(:linked_record__linked_records, :linked_records))
      .order(Sequel.asc(:heading))
  end

  private

  # As of now this only constructs a URL for authorized headings and then pings it to see what HTTP response code it gets back.
  # It would probably work but it's real slow and I don't know if I want to do it in a report, so for now I don't.
  def verify_source(source, id)
    locals = ["built", "local", "prov"]
    uri_prefix = {
      'aat' => "http://vocab.getty.edu/aat",
      'lcgft' => "http://id.loc.gov/authorities/genreForms",
      'lcnaf' => "http://id.loc.gov/authorities/names",
      'lcsh' => "http://id.loc.gov/authorities/subjects",
      'lcshg' => "http://id.loc.gov/authorities/subjects",
      'tgm' => "http://id.loc.gov/vocabulary/graphicMaterials",
      'tgn' => "http://vocab.getty.edu/tgn",
      'tucua' => "http://www2.archivists.org/thesaurus",
      'viaf' => "http://viaf.org/viaf"
    }

    unless locals.include?(source)
      url = "#{uri_prefix[source]}/#{id}"
      resp = Net::HTTP.get_response(URI.parse(url.to_s))
      return resp.code
    end
  end
end
