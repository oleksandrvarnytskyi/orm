from entity import *


class Article(Entity):
    _columns  = ['title', 'text']
    _parents  = ['category']
    _children = []
    _siblings = ['tags']


class Section(Entity):
    _columns  = ['title']
    _parents  = []
    _children = {'categories': 'Category'}
    _siblings = {}


class Category(Entity):
    _columns  = ['title']
    _parents  = ['section']
    _children = {'posts': 'Post'}
    _siblings = {}


class Post(Entity):
    _columns  = ['content', 'title']
    _parents  = ['category']
    _children = {'comments': 'Comment'}
    _siblings = {'tags': 'Tag'}

class Comment(Entity):
    _columns  = ['text']
    _parents  = ['post', 'user']
    _children = {}
    _siblings = {}

class Tag(Entity):
    _columns  = ['name']
    _parents  = []
    _children = {}
    _siblings = {'posts': 'Post'}

class User(Entity):
    _columns  = ['name', 'email', 'age']
    _parents  = []
    _children = {'comments': 'Comment'}
    _siblings = {}


if __name__ == "__main__":
    DATABASE_NAME = 'orm'
    USER_NAME = 'savar'
    try:
        Entity.db = psycopg2.connect(database=DATABASE_NAME, user=USER_NAME)

        # section = Section(2)
        # section.title = "zupa"
        # section.save()
        # print (section.title)

        # section2 = Section(3)
        # section2.title = "zupa"

        # category = Category(2)
        # category.title = 'Another title'
        # print(category.title)  # select from category where category_id=?
        # category.section = section2
        # print (category.section.id)# select from section where section_id=?
        # print(category)  # select from section where section_id=?

        # article = Article()
        # article.title = 'New title'
        # article.text = 'Very interesting content'
        # article.save()  # insert into article (article_title) values (?)
        # print article.id
        #
        # article = Article(8)
        # article.title = 'Another title'
        # article.text = 'Very interesting content with some freakin\' "quotes"'
        # article.save()
        # print article.created
        # print article.updated
        #
        # article = Article()
        # article.title = 'Third title'
        # article.text = 'Bugs are wonderful'
        # article.save()

        # article = Article(5)
        # article.title = 'New title'
        # article.text = 'Bugs are wonderful'
        # article.save()

        # article = Article(58)
        # article.delete()
        #
        for section in Section.all():
            print section.id, section.title
        #
        # section = Section(2)
        # section.title = "zupa"
        # for category in section.categories:  # select * from category where section_id=?
        #     print(category.title)
        #
        # post = Post(3)
        # post.content = "blow"
        # post.title = "wind"
        # for tag in post.tags:  # select * from tag natural join post_tag where post_id=?
        #     print tag.name

    finally:
        if Entity.db:
            Entity.db.close()
