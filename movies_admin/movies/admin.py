from django.contrib import admin
from .models import Genre, Filmwork, GenreFilmwork, Person, PersonFilmwork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    ...


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    ...


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork

    def changelist_view(self, request, extra_context=None):
        extra_context = {"title": "Change this for a custom title."}
        return super(GenreFilmworkInline, self).changelist_view(
            request, extra_context=extra_context
        )


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkInline)

    list_display = (
        # "title",
        "type",
        "creation_date",
        "rating",
    )

    list_filter = ("type",)

    search_fields = ("title", "description", "id")
