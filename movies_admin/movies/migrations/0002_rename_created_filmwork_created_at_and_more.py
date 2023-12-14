# Generated by Django 4.2.5 on 2023-12-14 19:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movies', '0001_initial'),
    ]

    operations = [
        migrations.RenameField(
            model_name='filmwork',
            old_name='created',
            new_name='created_at',
        ),
        migrations.RenameField(
            model_name='filmwork',
            old_name='modified',
            new_name='updated_at',
        ),
        migrations.RenameField(
            model_name='genre',
            old_name='created',
            new_name='created_at',
        ),
        migrations.RenameField(
            model_name='genre',
            old_name='modified',
            new_name='updated_at',
        ),
        migrations.RenameField(
            model_name='genrefilmwork',
            old_name='created',
            new_name='created_at',
        ),
        migrations.RenameField(
            model_name='person',
            old_name='created',
            new_name='created_at',
        ),
        migrations.RenameField(
            model_name='person',
            old_name='modified',
            new_name='updated_at',
        ),
        migrations.RenameField(
            model_name='personfilmwork',
            old_name='created',
            new_name='created_at',
        ),
        migrations.AddField(
            model_name='filmwork',
            name='file_path',
            field=models.CharField(blank=True, max_length=250, null=True),
        ),
    ]
