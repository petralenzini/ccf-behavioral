import PandasHelper as h



class KSADS:
    def __init__(self):
        pass

    def read_data(self, form):
        pass

    @staticmethod
    def warn_good_import(added, deleted, form, modified):
        if deleted.empty and modified.empty:
            h.showbox('''There are %s rows of new data and no unexpected changes to old data.
                            Please proceed with <code>data["raw"]</code>.''' % len(added),
                      form + ': Importing Data',
                      'success')
            h.showdataframe(added)

    @staticmethod
    def warn_modified(form, modified):
        if not modified.empty:
            h.showbox('''There are %s rows in the old data that has been modified in the new data.
                            If this is expected, you can ignore this message.
                            To further inspect rows type <code>data["modified"]</code>''' % len(modified),
                      form + ': Modified',
                      'danger')
            h.showdataframe(modified)

    @staticmethod
    def warn_deleted(deleted, form):
        if not deleted.empty:
            h.showbox('''There are %s rows in the old data that has been removed in the new data.
                            If this is expected, you can ignore this message.
                            To further inspect rows type <code>data["deleted"]</code>''' % len(deleted),
                      form + ': Deleted',
                      'danger')
            h.showdataframe(deleted)

    @staticmethod
    def warn_duplicates(duplicates, form):
        if duplicates.empty:
            h.showbox('''All patientid + patienttype combos are unique.''', form + ': No Duplicates', 'success')
        else:
            h.showbox('''There are %s rows that contain the same patientid + patienttype.''' % len(duplicates),
                      form + ': Duplicates',
                      'danger')
            h.showdataframe(duplicates)
            # raise Exception()

    @staticmethod
    def warn_not_in_redcap(not_in_redcap, form):
        if not_in_redcap.empty:
            h.showbox('''All patientid's are in Redcap.''', form + ': No Subject Missing from Redcap', 'success')
        else:
            h.showbox('''There are %s rows with patientid missing from Redcap.''' % len(not_in_redcap),
                      form + ': Subjects Missing from Redcap',
                      'danger')
            h.showdataframe(not_in_redcap)
            # raise Exception()

    @staticmethod
    def warn_missing(missing, form):
        if missing.empty:
            h.showbox('''All patientid's are in New Data.''', form + ': No Missing Redcap Subjects', 'success')
        else:
            h.showbox('''There are %s Redcap subjects missing from the current data.''' % len(missing),
                      form + ': Redcap Subjects Missing',
                      'danger')
            h.showdataframe(missing)
            # raise Exception()