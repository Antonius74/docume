DEFAULT_TAXONOMY_TREE: dict[str, dict[str, dict[str, list[str]]]] = {
    "Film e Cinema": {
        "Fantascienza": {
            "Stanley Kubrick": ["2001: Odissea nello spazio", "Arancia meccanica"],
            "Ridley Scott": ["Blade Runner", "Alien"],
            "Christopher Nolan": ["Interstellar", "Inception"],
        },
        "Commedia": {
            "Woody Allen": ["Annie Hall", "Manhattan"],
            "Roberto Benigni": ["La vita e bella", "Il mostro"],
        },
        "Dramma": {
            "Francis Ford Coppola": ["Il padrino", "Il padrino parte II"],
            "Martin Scorsese": ["Taxi Driver", "The Irishman"],
        },
        "Documentario": {
            "Werner Herzog": ["Grizzly Man", "Into the Abyss"],
            "Sconosciuto": ["Documentario storico", "Documentario scientifico"],
        },
    },
    "Libri e Documenti": {
        "Matematica e Statistica": {
            "Gilbert Strang": ["Linear Algebra", "MIT 18.06"],
            "David P. Doane": ["Applied Statistics", "Probability Distributions"],
            "Sconosciuto": ["Dispensa di algebra", "Slide di statistica"],
        },
        "Fisica e Scienze": {
            "Albert Einstein": ["Relativita ristretta", "Relativita generale"],
            "Richard Feynman": ["QED", "Feynman Lectures"],
            "Sconosciuto": ["Dispensa di fisica", "Materiale scientifico"],
        },
        "AI e Machine Learning": {
            "Andrew Ng": ["Machine Learning", "CS229 Notes"],
            "OpenAI": ["Guide LLM", "API documentation"],
            "Sconosciuto": ["Tutorial ML", "Dispensa AI"],
        },
        "Programmazione e Software": {
            "Martin Fowler": ["Refactoring", "Patterns of Enterprise Application Architecture"],
            "Robert C. Martin": ["Clean Code", "Clean Architecture"],
            "Sconosciuto": ["Manuale Python", "Documento tecnico software"],
        },
        "Legal e Compliance": {
            "Sconosciuto": ["Contratto", "Policy privacy", "Documento normativo"],
        },
        "Business e Finanza": {
            "Warren Buffett": ["Shareholder Letters"],
            "Aswath Damodaran": ["Valuation"],
            "Sconosciuto": ["Piano marketing", "Report finanziario"],
        },
    },
    "Siti Web e Articoli": {
        "News e Attualita": {
            "La Repubblica": ["Politica", "Cronaca", "Economia"],
            "Reuters": ["Breaking News", "World News"],
            "BBC": ["News", "Analysis"],
        },
        "Documentazione Tecnica": {
            "OpenAI": ["API docs", "Model guides"],
            "PostgreSQL": ["Documentation", "Manual"],
            "MDN": ["JavaScript docs", "Web APIs"],
        },
        "Blog Educativi": {
            "Real Python": ["Python Tutorials"],
            "Towards Data Science": ["Machine learning articles"],
            "Sconosciuto": ["Articolo didattico", "Guida tutorial"],
        },
        "Legal e Normativa": {
            "Garante Privacy": ["Provvedimenti", "Linee guida"],
            "EUR-Lex": ["Norme UE"],
            "Sconosciuto": ["Articolo legale", "Approfondimento compliance"],
        },
    },
    "Musica e Audio": {
        "Musica Classica": {
            "Johann Sebastian Bach": ["Cello Suites", "Arte della fuga"],
            "Ludwig van Beethoven": ["Sinfonia n.9", "Moonlight Sonata"],
        },
        "Rock e Metal": {
            "Metallica": ["Nothing Else Matters", "Master of Puppets"],
            "Pink Floyd": ["The Dark Side of the Moon"],
        },
        "Podcast e Interviste": {
            "Lex Fridman": ["Podcast Episodes"],
            "Joe Rogan": ["JRE Episodes"],
            "Sconosciuto": ["Intervista audio", "Podcast divulgativo"],
        },
    },
    "Immagini e Arte Visiva": {
        "Arte figurativa": {
            "Vincent van Gogh": ["Notte stellata", "I girasoli"],
            "Pablo Picasso": ["Guernica", "Les Demoiselles d'Avignon"],
        },
        "Natura e Paesaggi": {
            "Sconosciuto": ["Paesaggio naturale", "Fauna selvatica"],
        },
        "Infografiche e Slide": {
            "Sconosciuto": ["Infografica dati", "Slide didattica"],
        },
    },
    "Corsi e Formazione": {
        "Matematica e Statistica": {
            "Gilbert Strang": ["Corso Algebra Lineare"],
            "3Blue1Brown": ["Essence of Calculus", "Linear Algebra"],
            "StatQuest": ["Central Limit Theorem", "Regression"],
            "Sconosciuto": ["Corso di matematica", "Bootcamp statistica"],
        },
        "Fisica e Scienze": {
            "PBS Space Time": ["Special Relativity", "Quantum Mechanics"],
            "Leonard Susskind": ["General Relativity", "Quantum Field Theory"],
            "Sconosciuto": ["Corso di fisica", "Lezione di relativita"],
        },
        "AI e Machine Learning": {
            "Andrew Ng": ["Machine Learning Specialization"],
            "Sconosciuto": ["Corso AI", "Corso NLP"],
        },
        "Programmazione e Software": {
            "Traversy Media": ["JavaScript Crash Course", "React Course"],
            "The Net Ninja": ["Node.js Tutorials", "Vue Tutorials"],
            "Sconosciuto": ["Corso Python", "Corso Web Development"],
        },
    },
    "General": {
        "Generale": {
            "Sconosciuto": ["Contenuto non classificato"],
        },
    },
}
